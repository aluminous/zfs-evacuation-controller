//! Triggers: reconcile evacuation-worthy PVs into ZFSEvacuation CRs. All
//! state lives on the ZFSEvacuation; this loop only creates CRs.
//!
//! - Phase 1: a PV annotated `zfsevac.alumino.us/evacuate=true` — WhenIdle.
//! - Phase 2: a node tainted with the evacuate taint key — every zfs-localpv
//!   volume owned by that node is evacuated: WhenClaimed for volumes some
//!   pod references at that moment (the drain to come will leave their
//!   consumers Pending, and those consumers decide where the volumes go
//!   together), WhenIdle for the rest (nothing to co-locate them with).
//!   Both triggers lock the PVC immediately; the transfer waits for its
//!   last pod to go. Nothing is evicted here: draining is the operator's
//!   move, made after the lock.
//!
//! A periodic list (not a watch): a pure watcher misses the "ZFSEvacuation
//! was deleted while the trigger condition remains" case — no event fires, so
//! a failed evacuation could never be retried by deleting its CR.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{PersistentVolume, Pod};
use kube::api::{Api, ListParams};
use kube::ResourceExt;

use crate::controller::{node_has_evacuate_taint, node_id_of, pod_references_pvc, Ctx};
use crate::crd::openebs::{ZFSVolume, ZFS_DRIVER};
use crate::crd::zfs_evacuation::{
    EvacuationMode, EvacuationTrigger, Phase, ZFSEvacuation, ZFSEvacuationSpec,
    EVACUATE_ANNOTATION,
};

const POLL_INTERVAL: Duration = Duration::from_secs(15);

pub async fn run(ctx: Arc<Ctx>) -> anyhow::Result<()> {
    loop {
        if let Err(e) = tick(&ctx).await {
            tracing::warn!(error = %e, "trigger tick failed");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn tick(ctx: &Ctx) -> anyhow::Result<()> {
    let pvs: Api<PersistentVolume> = Api::all(ctx.client.clone());
    let pv_list = pvs.list(&ListParams::default()).await?;

    // Phase 1: annotated PVs.
    for pv in &pv_list {
        if wants_evacuation(pv) && clear_for_creation(ctx, &pv.name_any()).await {
            create_evacuation(
                ctx,
                &pv.name_any(),
                EvacuationTrigger::Annotation,
                EvacuationMode::WhenIdle,
            )
            .await;
        }
    }

    // Phase 2: tainted nodes. Map each tainted node's ZFSVolumes back to PVs
    // via volumeHandle (they diverge after a previous evacuation).
    let tainted_ids: Vec<String> = ctx
        .nodes()
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter(|n| node_has_evacuate_taint(n, &ctx.cfg.taint_key))
        .map(|n| node_id_of(&n))
        .collect();
    if tainted_ids.is_empty() {
        return Ok(());
    }
    let handle_to_pv: HashMap<String, &PersistentVolume> = pv_list
        .iter()
        .filter_map(|pv| {
            let csi = pv.spec.as_ref()?.csi.as_ref()?;
            (csi.driver == ZFS_DRIVER).then(|| (csi.volume_handle.clone(), pv))
        })
        .collect();
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    let mut pods_by_ns: HashMap<String, Vec<Pod>> = HashMap::new();
    for zv in zv_api.list(&ListParams::default()).await? {
        if !tainted_ids.contains(&zv.spec.0.owner_node_id) {
            continue;
        }
        let Some(pv) = handle_to_pv.get(&zv.name_any()) else {
            // Dataset with no PV (mid-migration artifacts, statically managed
            // volumes) — nothing for us to move.
            continue;
        };
        let pv_name = pv.name_any();
        if !clear_for_creation(ctx, &pv_name).await {
            continue;
        }
        let mode = if pv_in_use(ctx, pv, &mut pods_by_ns).await? {
            EvacuationMode::WhenClaimed
        } else {
            EvacuationMode::WhenIdle
        };
        create_evacuation(ctx, &pv_name, EvacuationTrigger::NodeTaint, mode).await;
    }
    Ok(())
}

/// Does any pod object reference the PV's claim right now? Pod lists are
/// per namespace and cached for the tick.
async fn pv_in_use(
    ctx: &Ctx,
    pv: &PersistentVolume,
    pods_by_ns: &mut HashMap<String, Vec<Pod>>,
) -> anyhow::Result<bool> {
    let Some(claim) = pv.spec.as_ref().and_then(|s| s.claim_ref.as_ref()) else {
        return Ok(false);
    };
    let (Some(ns), Some(pvc_name)) = (claim.namespace.as_deref(), claim.name.as_deref()) else {
        return Ok(false);
    };
    if !pods_by_ns.contains_key(ns) {
        let list = ctx.pods(ns).list(&ListParams::default()).await?;
        pods_by_ns.insert(ns.to_string(), list.items);
    }
    Ok(pods_by_ns[ns].iter().any(|p| pod_references_pvc(p, pvc_name)))
}

fn wants_evacuation(pv: &PersistentVolume) -> bool {
    let annotated = pv
        .annotations()
        .get(EVACUATE_ANNOTATION)
        .map(|v| v == "true")
        .unwrap_or(false);
    let is_zfs = pv
        .spec
        .as_ref()
        .and_then(|s| s.csi.as_ref())
        .map(|c| c.driver == ZFS_DRIVER)
        .unwrap_or(false);
    annotated && is_zfs && pv.metadata.deletion_timestamp.is_none()
}

/// Should a ZFSEvacuation be created for this PV? False while one is in
/// flight (or Failed), true when none exists or a Completed one was just
/// removed to make room.
async fn clear_for_creation(ctx: &Ctx, pv_name: &str) -> bool {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    match api.get_opt(pv_name).await {
        Ok(Some(existing)) => {
            // A Completed CR is a finished job keeping the per-PV name: the
            // trigger firing again (the volume's new node tainted, a fresh
            // annotation) is a new request, so replace it. Failed stays put —
            // deleting it is the operator's retry gesture, and it must stay
            // visible until then.
            let done = existing
                .status
                .as_ref()
                .is_some_and(|s| matches!(s.phase, Phase::Completed));
            if !done {
                return false;
            }
            match api.delete(pv_name, &Default::default()).await {
                Ok(_) => {
                    tracing::info!(pv = pv_name, "replaced Completed ZFSEvacuation");
                    true
                }
                Err(kube::Error::Api(e)) if e.code == 404 => true,
                Err(e) => {
                    tracing::warn!(pv = pv_name, error = %e, "failed to delete Completed ZFSEvacuation");
                    false
                }
            }
        }
        Ok(None) => true,
        Err(e) => {
            tracing::warn!(pv = pv_name, error = %e, "failed to check ZFSEvacuation");
            false
        }
    }
}

async fn create_evacuation(
    ctx: &Ctx,
    pv_name: &str,
    trigger: EvacuationTrigger,
    mode: EvacuationMode,
) {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let evac = ZFSEvacuation::new(
        pv_name,
        ZFSEvacuationSpec {
            pv_name: pv_name.to_string(),
            trigger: trigger.clone(),
            mode: mode.clone(),
            target_node: None,
            target_pool: None,
            settle_seconds: None,
            transfer_timeout_seconds: None,
            max_attempts: None,
            headroom_percent: None,
        },
    );
    match api.create(&Default::default(), &evac).await {
        Ok(_) => tracing::info!(pv = pv_name, ?trigger, ?mode, "created ZFSEvacuation"),
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => tracing::warn!(pv = pv_name, error = %e, "failed to create ZFSEvacuation"),
    }
}
