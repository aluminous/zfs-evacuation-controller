//! The evacuation state machine. Every phase is idempotent and resumable;
//! intent and recovery data are written to ZFSEvacuation `.status` before the
//! corresponding external mutation.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};
use serde_json::json;

use crate::controller::{
    abort, colocation, node_by_id, pod_is_scheduled, pod_names, pods_referencing_pvc,
    pv_swap, secs_since, target, Ctx, Error, now_rfc3339,
    parse_quantity_or_bytes, DEFAULT_SETTLE_SECONDS, LOCK_PROPAGATION_SECONDS,
};
use crate::crd::openebs::{
    ZFSSnapshot, ZFSVolume, MARKED_FOR_DELETION, ZFS_DRIVER,
    ZFS_STATUS_READY, ZFS_VOL_LABEL,
};
use crate::crd::zfs_evacuation::{
    ColocationRole, ColocationStatus, EvacuationMode, Phase, PvcRef, SourceInfo,
    ZFSEvacuation, ZFSEvacuationStatus, EVACUATE_ANNOTATION, EVACUATING_LABEL,
    EVACUATION_FINALIZER, GUARD_FINALIZER,
};
use crate::controller::transfer::{cleanup_transfer_crs, create_if_absent, destroy_target_snapshot, strip_zfs_finalizer_if_node_gone, transferring};

pub async fn reconcile(evac: Arc<ZFSEvacuation>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let name = evac.name_any();
    let mut st = evac.status.clone().unwrap_or_default();
    let deleted = evac.meta().deletion_timestamp.is_some();

    // Terminal phases: only finalizer removal remains.
    if matches!(st.phase, Phase::Completed | Phase::Failed) {
        if deleted {
            remove_evac_finalizer(&ctx, &name).await?;
        }
        return Ok(Action::await_change());
    }

    if deleted && !st.committed {
        // Deletion before the commit point: unwind, then let go.
        st.message = Some("ZFSEvacuation deleted before commit; aborting".into());
        return abort::run(&ctx, &evac, &mut st).await.map_err(Error::from);
    }
    // Past the commit point (deleted or not): roll forward only. Ensure our
    // finalizer while alive so deletion cannot orphan state.
    if !deleted {
        ensure_evac_finalizer(&ctx, &evac).await?;
    }

    if st.phase == Phase::Aborting {
        return abort::run(&ctx, &evac, &mut st).await.map_err(Error::from);
    }

    // Cancel checks before the commit point.
    if !st.committed
        && let Some(reason) = cancel_reason(&ctx, &evac, &st).await? {
            tracing::info!(evac = name, reason, "aborting evacuation");
            st.phase = Phase::Aborting;
            st.cancelled = true;
            st.message = Some(reason);
            ctx.write_status(&name, &st).await?;
            return Ok(Action::requeue(Duration::from_secs(1)));
        }

    let action = match st.phase {
        Phase::Pending => pending(&ctx, &evac, &mut st).await,
        Phase::Guarding => guarding(&ctx, &evac, &mut st).await,
        Phase::Retaining => retaining(&ctx, &evac, &mut st).await,
        Phase::Locking => locking(&ctx, &evac, &mut st).await,
        Phase::Quiescing => quiescing(&ctx, &evac, &mut st).await,
        Phase::TargetSelecting => target_selecting(&ctx, &evac, &mut st).await,
        Phase::Transferring => transferring(&ctx, &evac, &mut st).await,
        Phase::Adopting => adopting(&ctx, &evac, &mut st).await,
        Phase::Committing => pv_swap::committing(&ctx, &evac, &mut st).await,
        Phase::Swapping => pv_swap::swapping(&ctx, &evac, &mut st).await,
        Phase::CleaningUp => cleaning_up(&ctx, &evac, &mut st).await,
        Phase::Completed | Phase::Failed | Phase::Aborting => unreachable!(),
    }?;
    Ok(action)
}

pub fn error_policy(evac: Arc<ZFSEvacuation>, err: &Error, _ctx: Arc<Ctx>) -> Action {
    tracing::warn!(evac = evac.name_any(), error = %err, "reconcile error");
    Action::requeue(Duration::from_secs(15))
}

// ---------------------------------------------------------------- helpers

pub async fn advance(
    ctx: &Ctx,
    name: &str,
    st: &mut ZFSEvacuationStatus,
    phase: Phase,
) -> Result<Action> {
    tracing::info!(evac = name, ?phase, "phase transition");
    st.phase = phase;
    st.message = None;
    ctx.write_status(name, st).await?;
    Ok(Action::requeue(Duration::from_millis(500)))
}

pub async fn fail(
    ctx: &Ctx,
    name: &str,
    st: &mut ZFSEvacuationStatus,
    msg: impl Into<String>,
) -> Result<Action> {
    let msg = msg.into();
    tracing::warn!(evac = name, msg, "evacuation failed");
    st.phase = Phase::Failed;
    st.message = Some(msg);
    ctx.write_status(name, st).await?;
    Ok(Action::await_change())
}

pub async fn wait(
    ctx: &Ctx,
    name: &str,
    st: &mut ZFSEvacuationStatus,
    msg: impl Into<String>,
    secs: u64,
) -> Result<Action> {
    let msg = msg.into();
    if st.message.as_deref() != Some(msg.as_str()) {
        st.message = Some(msg);
        ctx.write_status(name, st).await?;
    }
    Ok(Action::requeue(Duration::from_secs(secs)))
}

async fn ensure_evac_finalizer(ctx: &Ctx, evac: &ZFSEvacuation) -> Result<()> {
    if evac.finalizers().iter().any(|f| f == EVACUATION_FINALIZER) {
        return Ok(());
    }
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let mut fins = evac.finalizers().to_vec();
    fins.push(EVACUATION_FINALIZER.into());
    api.patch(
        &evac.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({"metadata": {"finalizers": fins}})),
    )
    .await?;
    Ok(())
}

pub async fn remove_evac_finalizer(ctx: &Ctx, name: &str) -> Result<()> {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    if let Some(evac) = api.get_opt(name).await? {
        let fins: Vec<_> = evac
            .finalizers()
            .iter()
            .filter(|f| *f != EVACUATION_FINALIZER)
            .cloned()
            .collect();
        api.patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({"metadata": {"finalizers": fins}})),
        )
        .await?;
    }
    Ok(())
}

/// Returns Some(reason) when a pre-commit cancel condition holds.
async fn cancel_reason(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &ZFSEvacuationStatus,
) -> Result<Option<String>> {
    // In Pending nothing external has been touched yet; let pending() do its
    // own validation instead of aborting a not-yet-started evacuation.
    if st.phase == Phase::Pending {
        return Ok(None);
    }
    match ctx.pvs().get_opt(&evac.spec.pv_name).await? {
        None => return Ok(Some("source PV disappeared before commit".into())),
        Some(pv) => match evac.spec.trigger {
            crate::crd::zfs_evacuation::EvacuationTrigger::Annotation => {
                if pv.annotations().get(EVACUATE_ANNOTATION).map(String::as_str) != Some("true") {
                    return Ok(Some("evacuate annotation removed (cancelled)".into()));
                }
            }
            crate::crd::zfs_evacuation::EvacuationTrigger::NodeTaint => {
                // Cancel only when the source node still exists AND no longer
                // carries the taint. A vanished node is not a cancel — the
                // whole point is evacuating ahead of removal.
                if let Some(src) = &st.source
                    && let Some(node) = ctx.nodes().get_opt(&src.node).await?
                        && !crate::controller::node_has_evacuate_taint(&node, &ctx.cfg.taint_key) {
                            return Ok(Some("source node's evacuate taint removed (cancelled)".into()));
                        }
            }
        },
    }
    if let Some(pvc_ref) = &st.pvc_ref {
        match ctx
            .pvcs(&pvc_ref.namespace)
            .get_opt(&pvc_ref.name)
            .await?
        {
            None => return Ok(Some("PVC deleted during evacuation".into())),
            Some(pvc) => {
                if pvc.uid().as_deref() != Some(pvc_ref.uid.as_str()) {
                    return Ok(Some("PVC replaced (UID changed) during evacuation".into()));
                }
            }
        }
    }
    Ok(None)
}

/// Merge-patch a resource's finalizer list (arrays replace wholesale in merge
/// patch, which is what we want).
pub async fn set_finalizers<K>(
    api: &Api<K>,
    name: &str,
    f: impl Fn(&[String]) -> Vec<String>,
) -> Result<()>
where
    K: Resource + serde::de::DeserializeOwned + serde::Serialize + Clone + std::fmt::Debug,
    K::DynamicType: Default,
{
    if let Some(obj) = api.get_opt(name).await? {
        let cur = obj.meta().finalizers.clone().unwrap_or_default();
        let new = f(&cur);
        if new != cur {
            api.patch(
                name,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": {"finalizers": new}})),
            )
            .await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- phases

async fn pending(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let Some(pv) = ctx.pvs().get_opt(&evac.spec.pv_name).await? else {
        return fail(ctx, &name, st, format!("PV {} not found", evac.spec.pv_name)).await;
    };
    let spec = pv.spec.clone().unwrap_or_default();
    let Some(csi) = spec.csi.as_ref() else {
        return fail(ctx, &name, st, "PV is not a CSI volume").await;
    };
    if csi.driver != ZFS_DRIVER {
        return fail(ctx, &name, st, format!("PV driver {} is not {ZFS_DRIVER}", csi.driver)).await;
    }
    let handle = csi.volume_handle.clone();

    // Bound PVC with recorded identity.
    let Some(claim) = spec.claim_ref.as_ref() else {
        return fail(ctx, &name, st, "PV has no claimRef (unbound PVs need no evacuation)").await;
    };
    let (pvc_ns, pvc_name) = match (claim.namespace.clone(), claim.name.clone()) {
        (Some(ns), Some(n)) => (ns, n),
        _ => return fail(ctx, &name, st, "PV claimRef missing namespace/name").await,
    };
    let Some(pvc) = ctx.pvcs(&pvc_ns).get_opt(&pvc_name).await? else {
        return fail(ctx, &name, st, format!("bound PVC {pvc_ns}/{pvc_name} not found")).await;
    };
    if pvc.spec.as_ref().and_then(|s| s.volume_name.as_deref()) != Some(evac.spec.pv_name.as_str())
    {
        return fail(ctx, &name, st, "PVC does not reference this PV").await;
    }
    // Refuse pending resizes.
    let resizing = pvc
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter().any(|c| {
                (c.type_ == "Resizing" || c.type_ == "FileSystemResizePending")
                    && c.status == "True"
            })
        })
        .unwrap_or(false);
    if resizing {
        return fail(ctx, &name, st, "PVC has a resize in progress").await;
    }

    // Source ZFSVolume checks.
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    let Some(zv) = zv_api.get_opt(&handle).await? else {
        return fail(ctx, &name, st, format!("ZFSVolume {handle} not found")).await;
    };
    if zv.annotations().get(MARKED_FOR_DELETION).is_some() {
        return fail(ctx, &name, st, "ZFSVolume is marked for deletion").await;
    }
    if !zv.spec.0.snapname.as_deref().unwrap_or("").is_empty() {
        return fail(ctx, &name, st, "volume is a clone; clone migration is unsupported").await;
    }
    let snaps: Api<ZFSSnapshot> = ctx.openebs();
    let lp = ListParams::default().labels(&format!("{ZFS_VOL_LABEL}={handle}"));
    if !snaps.list(&lp).await?.items.is_empty() {
        return fail(
            ctx,
            &name,
            st,
            "volume has ZFSSnapshots; delete them before evacuating (snapshot migration unsupported)",
        )
        .await;
    }

    // Source node must exist to run zfs send.
    let source_node_id = zv.spec.0.owner_node_id.clone();
    let Some(src_node) = node_by_id(&ctx.nodes(), &source_node_id).await? else {
        return fail(
            ctx,
            &name,
            st,
            format!("source node with nodeid {source_node_id} not found; data is unreachable"),
        )
        .await;
    };

    let capacity_bytes = zv
        .spec
        .0
        .capacity
        .as_deref()
        .and_then(parse_quantity_or_bytes)
        .unwrap_or(0) as u64;
    if capacity_bytes == 0 {
        return fail(ctx, &name, st, "could not determine volume capacity").await;
    }

    st.pvc_ref = Some(PvcRef {
        namespace: pvc_ns,
        name: pvc_name,
        uid: pvc.uid().unwrap_or_default(),
    });
    st.source = Some(SourceInfo {
        node: src_node.name_any(),
        node_id: source_node_id,
        pool: zv.spec.0.pool_name.clone(),
        volume_handle: handle,
        pv_uid: pv.uid().unwrap_or_default(),
        capacity_bytes,
    });
    st.original_reclaim_policy = spec.persistent_volume_reclaim_policy.clone();
    advance(ctx, &name, st, Phase::Guarding).await
}

async fn guarding(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    set_finalizers(&zv_api, &source.volume_handle, |cur| {
        let mut v = cur.to_vec();
        if !v.iter().any(|f| f == GUARD_FINALIZER) {
            v.push(GUARD_FINALIZER.into());
        }
        v
    })
    .await
    .context("adding guard finalizer to source ZFSVolume")?;
    advance(ctx, &name, st, Phase::Retaining).await
}

async fn retaining(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    ctx.pvs()
        .patch(
            &evac.spec.pv_name,
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"persistentVolumeReclaimPolicy": "Retain"}})),
        )
        .await
        .context("patching PV reclaimPolicy to Retain")?;
    advance(ctx, &name, st, Phase::Locking).await
}

async fn locking(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let pvc_ref = st.pvc_ref.clone().ok_or_else(|| anyhow!("no pvcRef recorded"))?;
    // Both triggers lock immediately, in-use or not. What the lock denies
    // depends on the mode. WhenIdle: every pod creation — locking before
    // the consumer is evicted is what makes a drain safe, since the
    // workload controller's replacement pod is denied at creation instead of
    // landing as a never-scheduled pod pinned to the source by PV affinity,
    // which no creation-time policy can touch and which holds Quiescing
    // forever. WhenClaimed: only scheduler-bypassing pods — the replacement
    // is *meant* to exist as a Pending pod, and the cordon keeps it off the
    // source (Quiescing checks that). Either way: taint (or annotate) only
    // what you are about to drain.
    let key = format!("{}/{}", pvc_ref.namespace, pvc_ref.name);
    ctx.update_params(|params| params.lock(&key, &evac.spec.mode))
        .await
        .context("adding PVC to VAP params")?;
    // Human-visible marker only; the param object is the actual lock.
    ctx.pvcs(&pvc_ref.namespace)
        .patch(
            &pvc_ref.name,
            &PatchParams::default(),
            &Patch::Merge(json!({"metadata": {"labels": {EVACUATING_LABEL: "true"}}})),
        )
        .await
        .context("labeling PVC")?;
    st.locked_at = Some(now_rfc3339());
    advance(ctx, &name, st, Phase::Quiescing).await
}

async fn quiescing(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let pvc_ref = st.pvc_ref.clone().ok_or_else(|| anyhow!("no pvcRef recorded"))?;
    // VAP param propagation grace before trusting the lock.
    let since_lock = st.locked_at.as_deref().and_then(secs_since).unwrap_or(u64::MAX);
    if since_lock < LOCK_PROPAGATION_SECONDS {
        return Ok(Action::requeue(Duration::from_secs(
            LOCK_PROPAGATION_SECONDS - since_lock,
        )));
    }
    let pods = pods_referencing_pvc(&ctx.pods(&pvc_ref.namespace), &pvc_ref.name).await?;
    if let Some(msg) = blocking_reason(ctx, evac, st, &pods).await? {
        st.quiesced_at = None;
        return wait(ctx, &name, st, msg, 15).await;
    }
    let settle = evac.spec.settle_seconds.unwrap_or(DEFAULT_SETTLE_SECONDS);
    match st.quiesced_at.as_deref().and_then(secs_since) {
        None => {
            st.quiesced_at = Some(now_rfc3339());
            ctx.write_status(&name, st).await?;
            Ok(Action::requeue(Duration::from_secs(settle)))
        }
        Some(elapsed) if elapsed < settle => {
            Ok(Action::requeue(Duration::from_secs(settle - elapsed)))
        }
        Some(_) => advance(ctx, &name, st, Phase::TargetSelecting).await,
    }
}

/// Is the source node keeping scheduler-routed pods away (cordon, or the
/// evacuate taint with a hard effect)? A vanished node repels everything.
pub async fn source_repels_pods(ctx: &Ctx, st: &ZFSEvacuationStatus) -> Result<bool> {
    let source = st.source.as_ref().ok_or_else(|| anyhow!("no source recorded"))?;
    Ok(match ctx.nodes().get_opt(&source.node).await? {
        Some(node) => crate::controller::node_repels_pods(&node, &ctx.cfg.taint_key),
        None => true,
    })
}

/// Why the copy must not start (or, at commit, must not proceed) given the
/// pods referencing the PVC; None when it may. WhenIdle is strict: any pod
/// object blocks, scheduled or not — the remedies differ, so the message
/// tells them apart. WhenClaimed lets never-scheduled pods through *if* the
/// source node repels them; otherwise nothing would stop the scheduler from
/// binding the consumer to the source while the copy runs.
pub async fn blocking_reason(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &ZFSEvacuationStatus,
    pods: &[Pod],
) -> Result<Option<String>> {
    if evac.spec.mode == EvacuationMode::WhenIdle {
        return Ok(if pods.is_empty() {
            None
        } else {
            Some(crate::controller::blocking_pods_message(pods))
        });
    }
    // WhenClaimed. A scheduled pod always blocks.
    let scheduled: Vec<Pod> = pods.iter().filter(|p| pod_is_scheduled(p)).cloned().collect();
    if !scheduled.is_empty() {
        return Ok(Some(crate::controller::blocking_pods_message(&scheduled)));
    }
    // The cordon is required even with ZERO pods: claimablePvcKeys admits any
    // scheduler-routed pod, so a consumer created at any point mid-copy would
    // be pinned to the source by PV affinity and land there the moment the
    // node is schedulable. Only the node's own state closes that door.
    if source_repels_pods(ctx, st).await? {
        return Ok(None);
    }
    Ok(Some(match pods.is_empty() {
        true => "the source node is still schedulable; cordon it (kubectl cordon) or taint it \
                 with a NoSchedule effect so no consumer can land there mid-copy"
            .to_string(),
        false => format!(
            "consumer {} is waiting for this volume, but the source node is still schedulable; \
             cordon it (kubectl cordon) so the pod cannot land there mid-copy",
            pod_names(pods).join(", ")
        ),
    }))
}

async fn target_selecting(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let pvc_ref = st.pvc_ref.clone().ok_or_else(|| anyhow!("no pvcRef recorded"))?;

    // Serialize check-then-act across concurrent reconciles: without this,
    // two evacuations can both pass the caps/capacity checks and overcommit
    // the same source or target.
    let _selection_guard = ctx.selection_lock.lock().await;

    // Outbound concurrency cap: one active transfer per source node.
    let evacs: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let busy_source = evacs.list(&ListParams::default()).await?.items.into_iter().any(|e| {
        e.name_any() != name
            && e.status.as_ref().is_some_and(|s| {
                matches!(s.phase, Phase::Transferring | Phase::Adopting)
                    && s.source.as_ref().is_some_and(|src| src.node_id == source.node_id)
            })
    });
    if busy_source {
        return wait(ctx, &name, st, "another evacuation is transferring from this node", 60)
            .await;
    }

    let Some(pv) = ctx.pvs().get_opt(&evac.spec.pv_name).await? else {
        return fail(ctx, &name, st, "PV disappeared during target selection").await;
    };

    // WhenClaimed: the consumer waiting for this volume defines the group
    // that must land together. Resolved fresh on every attempt — a sibling
    // may have picked its target since the last one.
    let group = if evac.spec.mode == EvacuationMode::WhenClaimed {
        let pods = pods_referencing_pvc(&ctx.pods(&pvc_ref.namespace), &pvc_ref.name).await?;
        colocation::resolve(ctx, &name, &pvc_ref, &source.node_id, &pods).await?
    } else {
        None
    };
    let group_extra_bytes: u128 = group
        .as_ref()
        .map(|g| {
            g.members
                .iter()
                .filter(|m| m.pv_name != name)
                .map(|m| m.capacity_bytes as u128)
                .sum()
        })
        .unwrap_or(0);
    let input = target::SelectionInput {
        evac_name: &name,
        spec: &evac.spec,
        source_node_id: &source.node_id,
        source_pool: &source.pool,
        capacity_bytes: source.capacity_bytes as u128,
        pv: &pv,
        group_extra_bytes,
        anchor: group.as_ref().and_then(|g| g.anchor.as_ref()).map(|a| a.node.as_str()),
        placement: group.as_ref().map(|g| &g.placement),
    };
    match target::select_target(ctx, &input).await {
        Ok(mut t) => {
            t.new_volume_handle = format!(
                "{}-e{:06x}",
                source.volume_handle,
                rand::random::<u32>() & 0xff_ffff
            );
            st.colocation = group.map(|g| {
                let followed = g.anchor.filter(|_| evac.spec.target_node.is_none());
                tracing::info!(
                    evac = name,
                    pod = g.pod,
                    target = t.node,
                    members = ?g.members.iter().map(|m| &m.pv_name).collect::<Vec<_>>(),
                    followed = ?followed,
                    "co-location resolved"
                );
                ColocationStatus {
                    pod: g.pod,
                    members: g.members,
                    role: if followed.is_some() {
                        ColocationRole::Follower
                    } else {
                        ColocationRole::Leader
                    },
                    followed: followed.map(|a| a.pv_name),
                }
            });
            st.target = Some(t);
            advance(ctx, &name, st, Phase::Transferring).await
        }
        // Never split a group: a follower whose anchor cannot take it waits
        // for the anchor to free up (busy receiving, short on space) rather
        // than going elsewhere.
        Err(e) => wait(ctx, &name, st, format!("no target yet: {e:#}"), 60).await,
    }
}

async fn adopting(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let target = st.target.clone().ok_or_else(|| anyhow!("no target recorded"))?;
    let zv_api: Api<ZFSVolume> = ctx.openebs();

    match zv_api.get_opt(&target.new_volume_handle).await? {
        None => {
            let src = zv_api
                .get_opt(&source.volume_handle)
                .await?
                .ok_or_else(|| anyhow!("source ZFSVolume disappeared"))?;
            let zv = crate::controller::target_zfsvolume(src.spec.0.clone(), &target);
            create_if_absent(&zv_api, &zv).await?;
            Ok(Action::requeue(Duration::from_secs(3)))
        }
        Some(zv) => {
            let state = zv.status.as_ref().and_then(|s| s.state.clone()).unwrap_or_default();
            if state == ZFS_STATUS_READY {
                advance(ctx, &name, st, Phase::Committing).await
            } else if state == "Failed" {
                st.phase = Phase::Aborting;
                st.message = Some("new ZFSVolume reported Failed during adoption".into());
                ctx.write_status(&name, st).await?;
                Ok(Action::requeue(Duration::from_secs(1)))
            } else {
                wait(ctx, &name, st, "waiting for target node agent to adopt dataset", 5).await
            }
        }
    }
}

async fn cleaning_up(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;

    // 1. Transfer CRs first: the ZFSBackup finalizer destroys the transfer
    //    snapshot, which must happen before the dataset itself goes away.
    if let Some(t) = &st.transfer
        && !cleanup_transfer_crs(ctx, &name, t.attempt, st).await? {
            return wait(ctx, &name, st, "waiting for transfer CRs to clean up", 10).await;
        }

    // 2. The received stream recreated the transfer snapshot on the target
    //    (`<new dataset>@<snap>`); nothing else knows it exists. Destroy it
    //    before the source goes away, so a stuck target leaves both copies.
    if let Some(msg) = destroy_target_snapshot(ctx, &name, st).await? {
        return wait(ctx, &name, st, msg, 5).await;
    }

    // 3. Old ZFSVolume: issue delete, then drop our guard so the agent's
    //    destroy proceeds. Crash between the two resumes unambiguously
    //    (deletionTimestamp set + guard present -> remove guard).
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    if let Some(zv) = zv_api.get_opt(&source.volume_handle).await? {
        if zv.meta().deletion_timestamp.is_none() {
            zv_api
                .delete(&source.volume_handle, &DeleteParams::default())
                .await?;
        }
        set_finalizers(&zv_api, &source.volume_handle, |cur| {
            cur.iter().filter(|f| *f != GUARD_FINALIZER).cloned().collect()
        })
        .await?;
        strip_zfs_finalizer_if_node_gone(ctx, &zv_api, &source.volume_handle, &source.node_id)
            .await?;
        return wait(ctx, &name, st, "waiting for source dataset destruction", 10).await;
    }

    // 4. Unlock.
    if let Some(pvc_ref) = &st.pvc_ref {
        let key = format!("{}/{}", pvc_ref.namespace, pvc_ref.name);
        ctx.update_params(|params| params.unlock(&key)).await?;
        // PVC may legitimately be gone by now; ignore.
        let _ = ctx
            .pvcs(&pvc_ref.namespace)
            .patch(
                &pvc_ref.name,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": {"labels": {EVACUATING_LABEL: null}}})),
            )
            .await;
    }

    // 5. Make sure the replacement PV does not carry the evacuate trigger
    //    (an old stored manifest might), or deleting this CR would restart
    //    the whole evacuation.
    if let Some(pv) = ctx.pvs().get_opt(&evac.spec.pv_name).await?
        && pv.annotations().contains_key(EVACUATE_ANNOTATION) {
            ctx.pvs()
                .patch(
                    &evac.spec.pv_name,
                    &PatchParams::default(),
                    &Patch::Merge(json!({"metadata": {"annotations": {EVACUATE_ANNOTATION: null}}})),
                )
                .await?;
        }


    // Record Completed before dropping our finalizer: if the CR is mid-
    // deletion, the finalizer removal may erase the object immediately and a
    // later status write would 404.
    st.message = Some("evacuation complete".into());
    let action = advance(ctx, &name, st, Phase::Completed).await?;
    remove_evac_finalizer(ctx, &name).await?;
    Ok(action)
}
