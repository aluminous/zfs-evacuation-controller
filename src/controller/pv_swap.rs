//! Commit-point rendering and the PV swap itself. The swap window (old PV
//! deleted, new PV not yet created) is survivable only because both manifests
//! are durable in status before the first delete.

use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use k8s_openapi::api::core::v1::{ObjectReference, PersistentVolume};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};
use serde_json::json;

use crate::controller::state_machine::{advance, blocking_reason, fail, set_finalizers, wait};
use crate::controller::{pod_is_scheduled, pods_referencing_pvc, Ctx};
use crate::crd::openebs::{NODE_ID_TOPOLOGY_KEY, POOLNAME_ATTRIBUTE};
use crate::crd::zfs_evacuation::{EvacuationMode, Phase, ZFSEvacuation, ZFSEvacuationStatus};

const PV_PROTECTION: &str = "kubernetes.io/pv-protection";
const PROVISIONER_FINALIZER: &str = "external-provisioner.volume.kubernetes.io/finalizer";

pub async fn committing(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let pvc_ref = st.pvc_ref.clone().ok_or_else(|| anyhow!("no pvcRef recorded"))?;
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let target = st.target.clone().ok_or_else(|| anyhow!("no target recorded"))?;

    // Final invariant checks at the last cancelable moment.
    let pvc = ctx
        .pvcs(&pvc_ref.namespace)
        .get_opt(&pvc_ref.name)
        .await?
        .ok_or_else(|| anyhow!("PVC vanished at commit"))?;
    if pvc.uid().as_deref() != Some(pvc_ref.uid.as_str()) {
        st.phase = Phase::Aborting;
        st.message = Some("PVC UID changed at commit; aborting".into());
        ctx.write_status(&name, st).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }
    let pods = pods_referencing_pvc(&ctx.pods(&pvc_ref.namespace), &pvc_ref.name).await?;
    let scheduled: Vec<_> = pods.iter().filter(|p| pod_is_scheduled(p)).cloned().collect();
    let unexpected = match evac.spec.mode {
        // Should be impossible with the VAP lock.
        EvacuationMode::WhenIdle => pods.clone(),
        // The Pending consumer is expected; a *scheduled* one means the
        // source stopped repelling pods mid-copy and the consumer landed.
        EvacuationMode::WhenClaimed => scheduled,
    };
    if !unexpected.is_empty() {
        // Abort loudly rather than swap under a live consumer.
        st.phase = Phase::Aborting;
        st.message = Some(format!(
            "pods appeared at commit despite lock: {}",
            crate::controller::pod_names(&unexpected).join(", ")
        ));
        ctx.write_status(&name, st).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }
    // WhenClaimed with the source uncordoned: nothing is wrong yet, but the
    // swap window is exactly when the scheduler would bind the consumer to
    // the source. Hold here until the node repels pods again.
    if let Some(msg) = blocking_reason(ctx, evac, st, &pods).await? {
        return wait(ctx, &name, st, msg, 15).await;
    }

    let old_pv = ctx
        .pvs()
        .get_opt(&evac.spec.pv_name)
        .await?
        .ok_or_else(|| anyhow!("old PV vanished at commit"))?;
    if old_pv.uid().as_deref() != Some(source.pv_uid.as_str()) {
        return fail(ctx, &name, st, "old PV UID changed at commit").await;
    }

    let new_pv = render_new_pv(&old_pv, &pvc_ref, &target)?;

    st.old_pv_manifest = Some(serde_json::to_string(&old_pv)?);
    st.new_pv_manifest = Some(serde_json::to_string(&new_pv)?);
    st.committed = true;
    // The status write below is the commit record; from here, roll forward
    // only. Status in etcd is the sole store: deleting the CR mid-swap runs
    // the finalizer's roll-forward, and force-stripping the finalizer is an
    // accepted operator override, not something we insure against.
    advance(ctx, &name, st, Phase::Swapping).await
}

/// Render the replacement PV: same name (PVC bindings survive), new
/// volumeHandle/pool/nodeAffinity, pre-bound claimRef with the PVC's UID so
/// the PV controller rebinds automatically, and reclaimPolicy Retain until
/// the rebind is verified.
pub fn render_new_pv(
    old_pv: &PersistentVolume,
    pvc_ref: &crate::crd::zfs_evacuation::PvcRef,
    target: &crate::crd::zfs_evacuation::TargetInfo,
) -> Result<PersistentVolume> {
    // Keep annotations — pv.kubernetes.io/provisioned-by in particular, else
    // the external-provisioner will never reclaim the new PV and the target
    // dataset leaks at end-of-life. But drop our own evacuate trigger:
    // carrying it over would re-evacuate the volume as soon as the Completed
    // ZFSEvacuation is deleted.
    let annotations = old_pv.metadata.annotations.clone().map(|mut a| {
        a.remove(crate::crd::zfs_evacuation::EVACUATE_ANNOTATION);
        a
    });
    let mut new_pv = PersistentVolume {
        metadata: ObjectMeta {
            name: old_pv.metadata.name.clone(),
            labels: old_pv.metadata.labels.clone(),
            annotations,
            ..Default::default()
        },
        spec: old_pv.spec.clone(),
        status: None,
    };
    let spec = new_pv.spec.as_mut().ok_or_else(|| anyhow!("old PV has no spec"))?;
    spec.claim_ref = Some(ObjectReference {
        api_version: Some("v1".into()),
        kind: Some("PersistentVolumeClaim".into()),
        namespace: Some(pvc_ref.namespace.clone()),
        name: Some(pvc_ref.name.clone()),
        uid: Some(pvc_ref.uid.clone()),
        ..Default::default()
    });
    // Bring up as Retain; flipped back to the original policy after Bound.
    spec.persistent_volume_reclaim_policy = Some("Retain".into());
    spec.node_affinity = serde_json::from_value(json!({
        "required": {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": NODE_ID_TOPOLOGY_KEY,
                    "operator": "In",
                    "values": [target.node_id],
                }]
            }]
        }
    }))?;
    let csi = spec
        .csi
        .as_mut()
        .ok_or_else(|| anyhow!("old PV has no csi source"))?;
    csi.volume_handle = target.new_volume_handle.clone();
    if let Some(attrs) = csi.volume_attributes.as_mut()
        && attrs.contains_key(POOLNAME_ATTRIBUTE) {
            attrs.insert(POOLNAME_ATTRIBUTE.into(), target.pool.clone());
        }
    Ok(new_pv)
}

pub async fn swapping(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let pvc_ref = st.pvc_ref.clone().ok_or_else(|| anyhow!("no pvcRef recorded"))?;
    let pv_api = ctx.pvs();

    match pv_api.get_opt(&evac.spec.pv_name).await? {
        // Old PV still present: (re-)issue delete and strip the blocking
        // finalizers — pv-protection blocks while the PV is Bound (we've
        // verified quiescence), and the csi-provisioner's HonorPVReclaimPolicy
        // finalizer may also linger (safe to strip: the policy is Retain, so
        // the provisioner has no volume deletion to perform).
        Some(pv) if pv.uid().as_deref() == Some(source.pv_uid.as_str()) => {
            if pv.meta().deletion_timestamp.is_none() {
                // Re-verify right up against the delete: an uncordon after
                // Committing's check would let the scheduler bind the Pending
                // consumer to the source through the old PV's nodeAffinity in
                // exactly this window. Checking here (same reconcile, no
                // status write in between) shrinks the check-then-act gap to
                // the API round-trip; once the delete is issued the affinity
                // target is going away and holding no longer helps.
                let pods =
                    pods_referencing_pvc(&ctx.pods(&pvc_ref.namespace), &pvc_ref.name).await?;
                if let Some(msg) = blocking_reason(ctx, evac, st, &pods).await? {
                    return wait(ctx, &name, st, msg, 10).await;
                }
                pv_api.delete(&evac.spec.pv_name, &DeleteParams::default()).await?;
            }
            set_finalizers(&pv_api, &evac.spec.pv_name, |cur| {
                cur.iter()
                    .filter(|f| *f != PV_PROTECTION && *f != PROVISIONER_FINALIZER)
                    .cloned()
                    .collect()
            })
            .await?;
            Ok(Action::requeue(Duration::from_secs(2)))
        }
        // Gone: create the replacement from the stored manifest.
        None => {
            let manifest = st
                .new_pv_manifest
                .clone()
                .ok_or_else(|| anyhow!("no newPVManifest recorded; cannot complete swap"))?;
            let new_pv: PersistentVolume =
                serde_json::from_str(&manifest).context("parsing stored newPVManifest")?;
            match pv_api.create(&Default::default(), &new_pv).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 409 => {}
                Err(e) => return Err(e.into()),
            }
            Ok(Action::requeue(Duration::from_secs(2)))
        }
        // A PV with a different UID: our replacement. Wait for rebind.
        Some(pv) => {
            let phase = pv
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or_default();
            let claim_uid = pv
                .spec
                .as_ref()
                .and_then(|s| s.claim_ref.as_ref())
                .and_then(|c| c.uid.clone())
                .unwrap_or_default();
            if phase == "Bound" && claim_uid == pvc_ref.uid {
                if let Some(orig) = &st.original_reclaim_policy {
                    pv_api
                        .patch(
                            &evac.spec.pv_name,
                            &PatchParams::default(),
                            &Patch::Merge(
                                json!({"spec": {"persistentVolumeReclaimPolicy": orig}}),
                            ),
                        )
                        .await?;
                }
                tracing::info!(evac = name, "PV swap complete; PVC rebound");
                advance(ctx, &name, st, Phase::CleaningUp).await
            } else {
                Ok(Action::requeue(Duration::from_secs(3)))
            }
        }
    }
}
