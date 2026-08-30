//! The evacuation state machine. Every phase is idempotent and resumable;
//! intent and recovery data are written to ZFSEvacuation `.status` before the
//! corresponding external mutation.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};
use serde_json::json;

use crate::controller::{
    abort, node_by_id, node_ips, pods_referencing_pvc, pv_swap, secs_since, target, Ctx, Error,
    ActiveTransfer, now_rfc3339, parse_quantity_or_bytes, DEFAULT_MAX_ATTEMPTS,
    DEFAULT_SETTLE_SECONDS, DEFAULT_TRANSFER_TIMEOUT, LOCK_PROPAGATION_SECONDS,
};
use crate::crd::openebs::{
    VolumeInfo, ZFSBackup, ZFSBackupSpec, ZFSRestore, ZFSRestoreSpec, ZFSSnapshot, ZFSVolume,
    ZFSVolumeSpec, ZFSVolumeStatus, BKP_STATUS_DONE, BKP_STATUS_FAILED, BKP_STATUS_INIT,
    BKP_STATUS_INVALID, MARKED_FOR_DELETION, ZFS_DRIVER, ZFS_FINALIZER, ZFS_STATUS_PENDING,
    ZFS_STATUS_READY, ZFS_VOL_LABEL,
};
use crate::crd::zfs_evacuation::{
    Phase, PvcRef, SourceInfo, TransferStatus, ZFSEvacuation, ZFSEvacuationStatus,
    EVACUATE_ANNOTATION, EVACUATING_LABEL, EVACUATION_FINALIZER, GUARD_FINALIZER,
};
use crate::transfer::relay::Relay;

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

async fn wait(
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
        Some(pv) => {
            if pv.annotations().get(EVACUATE_ANNOTATION).map(String::as_str) != Some("true") {
                return Ok(Some("evacuate annotation removed (cancelled)".into()));
            }
        }
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
    let key = format!("{}/{}", pvc_ref.namespace, pvc_ref.name);
    ctx.update_params(|keys| {
        if keys.contains(&key) {
            false
        } else {
            keys.push(key.clone());
            true
        }
    })
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
    if !pods.is_empty() {
        st.quiesced_at = None;
        return wait(
            ctx,
            &name,
            st,
            format!("waiting for pods to release PVC: {}", pods.join(", ")),
            15,
        )
        .await;
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

async fn target_selecting(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;

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
    let input = target::SelectionInput {
        evac_name: &name,
        spec: &evac.spec,
        source_node_id: &source.node_id,
        source_pool: &source.pool,
        capacity_bytes: source.capacity_bytes as u128,
        pv: &pv,
    };
    match target::select_target(ctx, &input).await {
        Ok(mut t) => {
            t.new_volume_handle = format!(
                "{}-e{:06x}",
                source.volume_handle,
                rand::random::<u32>() & 0xff_ffff
            );
            st.target = Some(t);
            advance(ctx, &name, st, Phase::Transferring).await
        }
        Err(e) => wait(ctx, &name, st, format!("no target yet: {e:#}"), 60).await,
    }
}

pub fn bkp_name(evac: &str, attempt: u32) -> String {
    format!("{evac}-evac-a{attempt}")
}
pub fn rst_name(evac: &str, attempt: u32) -> String {
    format!("{evac}-evacr-a{attempt}")
}

async fn transferring(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    let max_attempts = evac.spec.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let timeout = evac
        .spec
        .transfer_timeout_seconds
        .unwrap_or(DEFAULT_TRANSFER_TIMEOUT);

    let Some(t) = st.transfer.clone() else {
        return start_attempt(ctx, evac, st, 1).await;
    };

    // CR statuses come FIRST: a transfer that completed while we weren't
    // looking (e.g. across a controller restart) must advance, not be torn
    // down and re-copied.
    let bkp_api: Api<ZFSBackup> = ctx.openebs();
    let rst_api: Api<ZFSRestore> = ctx.openebs();
    let bkp = bkp_api.get_opt(&bkp_name(&name, t.attempt)).await?;
    let rst = rst_api.get_opt(&rst_name(&name, t.attempt)).await?;
    let bkp_status = bkp.as_ref().and_then(|b| b.status.clone()).unwrap_or_default();
    let rst_status = rst.as_ref().and_then(|r| r.status.clone()).unwrap_or_default();

    // Snapshot of in-memory relay state (guard dropped before any await).
    let mem = {
        let map = ctx.transfers.lock().unwrap();
        map.get(&name).map(|t| {
            (
                t.attempt,
                t.relay.bytes.load(std::sync::atomic::Ordering::Relaxed),
                t.relay
                    .last_activity
                    .load(std::sync::atomic::Ordering::Relaxed),
                t.relay.task.is_finished(),
            )
        })
    };

    if bkp_status == BKP_STATUS_DONE && rst_status == BKP_STATUS_DONE {
        drop_relay(ctx, &name);
        if let Some((_, bytes, _, _)) = mem {
            st.transfer.as_mut().unwrap().bytes_relayed = Some(bytes);
        }
        tracing::info!(evac = name, bytes = ?st.transfer.as_ref().and_then(|x| x.bytes_relayed), "transfer complete");
        return advance(ctx, &name, st, Phase::Adopting).await;
    }
    for (role, s) in [("backup", &bkp_status), ("restore", &rst_status)] {
        if s == BKP_STATUS_FAILED || s == BKP_STATUS_INVALID {
            return retry_or_fail(
                ctx,
                evac,
                st,
                &t,
                max_attempts,
                &format!("{role} reported status {s}"),
            )
            .await;
        }
    }
    let elapsed = t.started_at.as_deref().and_then(secs_since).unwrap_or(0);
    if elapsed > timeout {
        return retry_or_fail(ctx, evac, st, &t, max_attempts, "transfer timed out").await;
    }

    let Some((mem_attempt, bytes, last_activity, task_finished)) = mem else {
        // A recorded, non-terminal attempt with no live relay: the controller
        // restarted mid-transfer. Invalidate the attempt.
        return retry_or_fail(ctx, evac, st, &t, max_attempts, "controller restarted mid-transfer")
            .await;
    };
    // Reconciles can carry a STALE cached status (an event older than our own
    // last write). A live relay newer than the status view must never be
    // killed — that cascades into burning every attempt. Only a relay OLDER
    // than the recorded attempt is stale and safe to drop.
    if mem_attempt > t.attempt {
        return Ok(Action::requeue(Duration::from_secs(2)));
    }
    if mem_attempt < t.attempt {
        drop_relay(ctx, &name);
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Stall detection only applies while the stream is live: once the relay
    // task has finished, all bytes are delivered and we're waiting on the
    // agents' status flips (recv finalization can legitimately take a while;
    // the per-attempt timeout bounds it).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if !task_finished && now.saturating_sub(last_activity) > ctx.cfg.stall_seconds {
        return retry_or_fail(ctx, evac, st, &t, max_attempts, "transfer stalled (no bytes)")
            .await;
    }

    // Progress heartbeat.
    if st.transfer.as_ref().and_then(|x| x.bytes_relayed) != Some(bytes) {
        st.transfer.as_mut().unwrap().bytes_relayed = Some(bytes);
        ctx.write_status(&name, st).await?;
    }
    Ok(Action::requeue(Duration::from_secs(10)))
}

fn drop_relay(ctx: &Ctx, name: &str) {
    if let Some(t) = ctx.transfers.lock().unwrap().remove(name) {
        t.relay.task.abort();
    }
}

/// Tear down a failed attempt's CRs; once both are gone, start the next
/// attempt or give up.
async fn retry_or_fail(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
    t: &TransferStatus,
    max_attempts: u32,
    reason: &str,
) -> Result<Action> {
    let name = evac.name_any();
    drop_relay(ctx, &name);
    let gone = cleanup_transfer_crs(ctx, &name, t.attempt, st).await?;
    if !gone {
        return wait(ctx, &name, st, format!("attempt {} failed ({reason}); cleaning up", t.attempt), 10)
            .await;
    }
    if t.attempt >= max_attempts {
        st.phase = Phase::Aborting;
        st.message = Some(format!(
            "transfer failed after {} attempts (last: {reason})",
            t.attempt
        ));
        ctx.write_status(&name, st).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }
    tracing::warn!(evac = name, attempt = t.attempt, reason, "transfer attempt failed; retrying");
    start_attempt(ctx, evac, st, t.attempt + 1).await
}

/// Delete this attempt's ZFSBackup/ZFSRestore; returns true once both are
/// gone. Strips stuck zfs finalizers when the owning node no longer exists.
pub async fn cleanup_transfer_crs(
    ctx: &Ctx,
    evac_name: &str,
    attempt: u32,
    st: &ZFSEvacuationStatus,
) -> Result<bool> {
    let bkp_api: Api<ZFSBackup> = ctx.openebs();
    let rst_api: Api<ZFSRestore> = ctx.openebs();
    let mut all_gone = true;
    let source_node_id = st.source.as_ref().map(|s| s.node_id.clone());
    let target_node_id = st.target.as_ref().map(|t| t.node_id.clone());

    let b = bkp_name(evac_name, attempt);
    if let Some(cr) = bkp_api.get_opt(&b).await? {
        all_gone = false;
        if cr.meta().deletion_timestamp.is_none() {
            bkp_api.delete(&b, &DeleteParams::default()).await?;
        } else if let Some(nid) = &source_node_id {
            strip_zfs_finalizer_if_node_gone(ctx, &bkp_api, &b, nid).await?;
        }
    }
    let r = rst_name(evac_name, attempt);
    if let Some(cr) = rst_api.get_opt(&r).await? {
        all_gone = false;
        if cr.meta().deletion_timestamp.is_none() {
            rst_api.delete(&r, &DeleteParams::default()).await?;
        } else if let Some(nid) = &target_node_id {
            strip_zfs_finalizer_if_node_gone(ctx, &rst_api, &r, nid).await?;
        }
    }
    Ok(all_gone)
}

/// If the node that should process a CR's zfs finalizer no longer exists,
/// remove the finalizer ourselves — the disk is gone with the node; leaking
/// the on-disk artifact is the correct outcome.
pub async fn strip_zfs_finalizer_if_node_gone<K>(
    ctx: &Ctx,
    api: &Api<K>,
    name: &str,
    node_id: &str,
) -> Result<()>
where
    K: Resource + serde::de::DeserializeOwned + serde::Serialize + Clone + std::fmt::Debug,
    K::DynamicType: Default,
{
    if node_by_id(&ctx.nodes(), node_id).await?.is_some() {
        return Ok(());
    }
    tracing::warn!(cr = name, node_id, "owner node gone; stripping zfs finalizer");
    set_finalizers(api, name, |cur| {
        cur.iter().filter(|f| *f != ZFS_FINALIZER).cloned().collect()
    })
    .await
}

async fn start_attempt(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
    attempt: u32,
) -> Result<Action> {
    let name = evac.name_any();
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let target = st.target.clone().ok_or_else(|| anyhow!("no target recorded"))?;

    // Idempotent re-entry: a stale-status reconcile can land here for an
    // attempt that is already running — never respawn over a live relay.
    {
        let map = ctx.transfers.lock().unwrap();
        if let Some(existing) = map.get(&name)
            && existing.attempt >= attempt {
                return Ok(Action::requeue(Duration::from_secs(5)));
            }
    }

    let src_node = node_by_id(&ctx.nodes(), &source.node_id)
        .await?
        .ok_or_else(|| anyhow!("source node {} gone", source.node_id))?;
    let dst_node = node_by_id(&ctx.nodes(), &target.node_id)
        .await?
        .ok_or_else(|| anyhow!("target node {} gone", target.node_id))?;

    let relay = Relay::spawn(
        relay_peers(ctx, &src_node).await?,
        relay_peers(ctx, &dst_node).await?,
    )
    .await?;
    // Register immediately: from here on, any error path can find and abort
    // the relay (an unregistered relay task would leak its sockets forever).
    {
        let mut map = ctx.transfers.lock().unwrap();
        if let Some(old) = map.remove(&name) {
            old.relay.task.abort();
        }
        map.insert(name.clone(), ActiveTransfer { attempt, relay });
    }
    let (backup_port, restore_port) = {
        let map = ctx.transfers.lock().unwrap();
        let t = &map[&name].relay;
        (t.backup_port, t.restore_port)
    };
    let snap_name = format!("zevac-a{attempt}-{:04x}", rand::random::<u16>());

    // Intent before action: the attempt record must be durable before the CRs
    // exist, so a crash always knows what to clean up.
    st.transfer = Some(TransferStatus {
        attempt,
        snap_name: snap_name.clone(),
        ports: vec![backup_port, restore_port],
        bytes_relayed: None,
        started_at: Some(now_rfc3339()),
    });
    st.message = Some(format!("transfer attempt {attempt} starting"));
    ctx.write_status(&name, st).await?;

    // Adapted volSpec for the receive side.
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    let zv = zv_api
        .get_opt(&source.volume_handle)
        .await?
        .ok_or_else(|| anyhow!("source ZFSVolume disappeared"))?;
    let mut vol_spec: VolumeInfo = zv.spec.0.clone();
    vol_spec.owner_node_id = target.node_id.clone();
    vol_spec.pool_name = target.pool.clone();
    vol_spec.snapname = None;

    // Restore first (its listener side has no ordering requirement, but both
    // must find the relay listening — which it already is).
    let rst_api: Api<ZFSRestore> = ctx.openebs();
    let mut rst = ZFSRestore::new(
        &rst_name(&name, attempt),
        ZFSRestoreSpec {
            volume_name: target.new_volume_handle.clone(),
            owner_node_id: target.node_id.clone(),
            restore_src: format!("{}:{}", ctx.cfg.pod_ip, restore_port),
        },
        vol_spec,
    );
    rst.status = Some(BKP_STATUS_INIT.to_string());
    create_if_absent(&rst_api, &rst).await?;

    let bkp_api: Api<ZFSBackup> = ctx.openebs();
    let mut bkp = ZFSBackup::new(
        &bkp_name(&name, attempt),
        ZFSBackupSpec {
            volume_name: source.volume_handle.clone(),
            owner_node_id: source.node_id.clone(),
            snap_name: Some(snap_name),
            prev_snap_name: None,
            backup_dest: format!("{}:{}", ctx.cfg.pod_ip, backup_port),
        },
    );
    bkp.status = Some(BKP_STATUS_INIT.to_string());
    create_if_absent(&bkp_api, &bkp).await?;
    Ok(Action::requeue(Duration::from_secs(5)))
}

/// Addresses the relay should accept from for a given node: the node's own
/// addresses (agents on hostNetwork, or SNAT'd egress) plus the IPs of pods
/// in the openebs namespace running on that node (agents on the pod network
/// whose CNI preserves source IPs).
async fn relay_peers(
    _ctx: &Ctx,
    node: &k8s_openapi::api::core::v1::Node,
) -> Result<Vec<crate::transfer::relay::IpNet>> {
    use crate::transfer::relay::IpNet;
    // Node addresses cover hostNetwork agents and SNAT'd egress; the node's
    // pod CIDR(s) cover agents on the pod network AND the CNI gateway address
    // the traffic can be masqueraded to when crossing nodes (observed in
    // practice: flannel presents <podCIDR>.1 as the source).
    let mut peers: Vec<IpNet> = node_ips(node).into_iter().map(IpNet::host).collect();
    if let Some(spec) = node.spec.as_ref() {
        let mut cidrs: Vec<String> = spec.pod_cidrs.clone().unwrap_or_default();
        if let Some(c) = spec.pod_cidr.clone()
            && !cidrs.contains(&c) {
                cidrs.push(c);
            }
        peers.extend(cidrs.iter().filter_map(|c| IpNet::parse_cidr(c)));
    }
    if peers.is_empty() {
        return Err(anyhow!("no relay peer addresses found for node {}", node.name_any()));
    }
    Ok(peers)
}

pub async fn create_if_absent<K>(api: &Api<K>, obj: &K) -> Result<()>
where
    K: Resource + serde::de::DeserializeOwned + serde::Serialize + Clone + std::fmt::Debug,
    K::DynamicType: Default,
{
    match api.create(&Default::default(), obj).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
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
            let mut info = src.spec.0.clone();
            info.owner_node_id = target.node_id.clone();
            info.pool_name = target.pool.clone();
            info.snapname = None;
            let mut zv = ZFSVolume::new(&target.new_volume_handle, ZFSVolumeSpec(info));
            // The node agent lists its volumes by this label.
            zv.meta_mut()
                .labels
                .get_or_insert_with(Default::default)
                .insert("kubernetes.io/nodename".into(), target.node_id.clone());
            zv.status = Some(ZFSVolumeStatus {
                state: Some(ZFS_STATUS_PENDING.to_string()),
            });
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

    // 2. Old ZFSVolume: issue delete, then drop our guard so the agent's
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

    // 3. Unlock.
    if let Some(pvc_ref) = &st.pvc_ref {
        let key = format!("{}/{}", pvc_ref.namespace, pvc_ref.name);
        ctx.update_params(|keys| {
            let before = keys.len();
            keys.retain(|k| k != &key);
            keys.len() != before
        })
        .await?;
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

    // 4. Make sure the replacement PV does not carry the evacuate trigger
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

    // 5. Recovery ConfigMap.
    pv_swap::delete_recovery_configmap(ctx, &name).await?;

    // Record Completed before dropping our finalizer: if the CR is mid-
    // deletion, the finalizer removal may erase the object immediately and a
    // later status write would 404.
    st.message = Some("evacuation complete".into());
    let action = advance(ctx, &name, st, Phase::Completed).await?;
    remove_evac_finalizer(ctx, &name).await?;
    Ok(action)
}
