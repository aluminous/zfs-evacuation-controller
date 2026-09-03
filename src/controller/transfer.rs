//! The Transferring phase: per-attempt relay lifecycle, the pure verdict
//! that orders success/failure/staleness/timeout/stall, ZFSBackup/ZFSRestore
//! CR management, and the target-side transfer-snapshot cleanup used by
//! CleaningUp and the abort path.

use std::time::Duration;

use anyhow::{anyhow, Result};
use kube::api::{Api, DeleteParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};

use crate::controller::state_machine::{advance, set_finalizers, wait};
use crate::controller::{
    node_by_id, node_ips, secs_since, Ctx, ActiveTransfer, now_rfc3339,
    DEFAULT_MAX_ATTEMPTS, DEFAULT_TRANSFER_TIMEOUT,
};
use crate::crd::openebs::{
    VolumeInfo, ZFSBackup, ZFSBackupSpec, ZFSRestore, ZFSRestoreSpec, ZFSSnapshot, ZFSVolume,
    ZFSVolumeStatus, BKP_STATUS_DONE, BKP_STATUS_FAILED, BKP_STATUS_INIT, BKP_STATUS_INVALID,
    ZFS_FINALIZER, ZFS_STATUS_PENDING, ZFS_VOL_LABEL,
};
use crate::crd::zfs_evacuation::{Phase, TransferStatus, ZFSEvacuation, ZFSEvacuationStatus};
use crate::transfer::relay::Relay;

pub fn bkp_name(evac: &str, attempt: u32) -> String {
    format!("{evac}-evac-a{attempt}")
}
pub fn rst_name(evac: &str, attempt: u32) -> String {
    format!("{evac}-evacr-a{attempt}")
}

pub async fn transferring(
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

    let bkp_api: Api<ZFSBackup> = ctx.openebs();
    let rst_api: Api<ZFSRestore> = ctx.openebs();
    let bkp = bkp_api.get_opt(&bkp_name(&name, t.attempt)).await?;
    let rst = rst_api.get_opt(&rst_name(&name, t.attempt)).await?;

    // Snapshot of in-memory relay state (guard dropped before any await).
    let mem = {
        let map = ctx.transfers.lock().unwrap();
        map.get(&name).map(|a| RelayObs {
            attempt: a.attempt,
            bytes: a.relay.bytes.load(std::sync::atomic::Ordering::Relaxed),
            last_activity: a
                .relay
                .last_activity
                .load(std::sync::atomic::Ordering::Relaxed),
            task_finished: a.relay.task.is_finished(),
            source_connected: a
                .relay
                .source_connected
                .load(std::sync::atomic::Ordering::Acquire),
        })
    };

    let obs = TransferObs {
        attempt: t.attempt,
        failure_reason: t.failure_reason.clone(),
        bkp_status: bkp.as_ref().and_then(|b| b.status.clone()).unwrap_or_default(),
        rst_status: rst.as_ref().and_then(|r| r.status.clone()).unwrap_or_default(),
        rst_exists: rst.is_some(),
        mem,
        elapsed_secs: t.started_at.as_deref().and_then(secs_since).unwrap_or(0),
        now_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        timeout_secs: timeout,
        stall_secs: ctx.cfg.stall_seconds,
    };
    match transfer_verdict(&obs) {
        TransferVerdict::Complete { bytes } => {
            drop_relay(ctx, &name);
            if let Some(bytes) = bytes {
                st.transfer.as_mut().unwrap().bytes_relayed = Some(bytes);
            }
            tracing::info!(evac = name, bytes = ?st.transfer.as_ref().and_then(|x| x.bytes_relayed), "transfer complete");
            advance(ctx, &name, st, Phase::Adopting).await
        }
        TransferVerdict::FailAttempt(reason) => {
            retry_or_fail(ctx, evac, st, &t, max_attempts, &reason).await
        }
        TransferVerdict::StaleStatus => Ok(Action::requeue(Duration::from_secs(2))),
        TransferVerdict::StaleRelay => {
            drop_relay(ctx, &name);
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        TransferVerdict::WaitingForSource => {
            wait(ctx, &name, st, format!("transfer attempt {} waiting for source", t.attempt), 2)
                .await
        }
        TransferVerdict::CreateRestore => {
            create_restore(ctx, st, &name, t.attempt, t.ports.get(1).copied().unwrap_or(0))
                .await?;
            Ok(Action::requeue(Duration::from_secs(2)))
        }
        TransferVerdict::Continue { bytes } => {
            if st.transfer.as_ref().and_then(|x| x.bytes_relayed) != Some(bytes) {
                st.transfer.as_mut().unwrap().bytes_relayed = Some(bytes);
                ctx.write_status(&name, st).await?;
            }
            Ok(Action::requeue(Duration::from_secs(10)))
        }
    }
}

/// Everything the transfer phase can observe about an attempt, gathered in
/// one struct so the precedence between success, failure, staleness, the
/// attempt timeout and the stall detector lives in exactly one (pure,
/// testable) place: [`transfer_verdict`].
pub struct TransferObs {
    pub attempt: u32,
    pub failure_reason: Option<String>,
    pub bkp_status: String,
    pub rst_status: String,
    pub rst_exists: bool,
    pub mem: Option<RelayObs>,
    pub elapsed_secs: u64,
    pub now_secs: u64,
    pub timeout_secs: u64,
    pub stall_secs: u64,
}

pub struct RelayObs {
    pub attempt: u32,
    /// Bytes DELIVERED TO THE TARGET (the relay counts drain-side writes),
    /// so bytes > 0 implies the target is connected and receiving.
    pub bytes: u64,
    pub last_activity: u64,
    pub task_finished: bool,
    pub source_connected: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TransferVerdict {
    Complete { bytes: Option<u64> },
    FailAttempt(String),
    /// Status view older than the live relay: touch nothing.
    StaleStatus,
    /// Relay older than the recorded attempt: drop it.
    StaleRelay,
    WaitingForSource,
    CreateRestore,
    Continue { bytes: u64 },
}

pub fn transfer_verdict(o: &TransferObs) -> TransferVerdict {
    // Success wins over everything, a recorded failure_reason included: a
    // teardown that loses the race against the agents' Done flips must
    // salvage the received data, not destroy it.
    if o.bkp_status == BKP_STATUS_DONE && o.rst_status == BKP_STATUS_DONE {
        return TransferVerdict::Complete { bytes: o.mem.as_ref().map(|m| m.bytes) };
    }
    // An attempt already judged failed is only ever torn down; re-deriving a
    // verdict from its half-deleted CRs and dropped relay would replace the
    // real reason with a bogus one ("controller restarted").
    if let Some(r) = &o.failure_reason {
        return TransferVerdict::FailAttempt(r.clone());
    }
    for (role, s) in [("backup", &o.bkp_status), ("restore", &o.rst_status)] {
        if s == BKP_STATUS_FAILED || s == BKP_STATUS_INVALID {
            return TransferVerdict::FailAttempt(format!("{role} reported status {s}"));
        }
    }
    let Some(m) = &o.mem else {
        // A recorded, non-terminal attempt with no live relay: the controller
        // restarted mid-transfer. Invalidate the attempt.
        return TransferVerdict::FailAttempt("controller restarted mid-transfer".into());
    };
    // Reconciles can carry a STALE cached status (an event older than our own
    // last write). A live relay newer than the status view must never be
    // killed — that cascades into burning every attempt. Only a relay OLDER
    // than the recorded attempt is stale and safe to drop.
    if m.attempt > o.attempt {
        return TransferVerdict::StaleStatus;
    }
    if m.attempt < o.attempt {
        return TransferVerdict::StaleRelay;
    }
    // One clock bounds every non-flowing state: waiting for the source,
    // waiting for the target (including a source that already EOF'd into the
    // buffer), and the agents' post-stream finalization. Only a stream
    // actually delivering bytes to the target is exempt — tearing down a
    // progressing transfer only to restart it from zero can never finish.
    let flowing = !m.task_finished && m.bytes > 0;
    if !flowing && o.elapsed_secs > o.timeout_secs {
        return TransferVerdict::FailAttempt("transfer timed out".into());
    }
    // The target is dialed only once the source is connected and flowing:
    // the agents' `nc -w 3` closes any connection idle for 3 s, so a target
    // that connects before the source has bytes dies before the stream
    // starts (and burns an attempt in exactly 3 s).
    if !o.rst_exists {
        return if m.source_connected {
            TransferVerdict::CreateRestore
        } else {
            TransferVerdict::WaitingForSource
        };
    }
    // Stall detection applies only to a flowing stream (bytes reaching the
    // target) that stops moving. Before the target connects, last_activity
    // freezes at the source's EOF / full buffer — that state belongs to the
    // attempt timeout above, not to the stall detector.
    if flowing && o.now_secs.saturating_sub(m.last_activity) > o.stall_secs {
        return TransferVerdict::FailAttempt("transfer stalled (no bytes)".into());
    }
    TransferVerdict::Continue { bytes: m.bytes }
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
    if let Some(x) = st.transfer.as_mut() {
        x.failure_reason = Some(reason.to_string());
    }
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

/// The ZFSSnapshot CR that stands in for the target-side copy of the transfer
/// snapshot: the agent on the target node names the dataset from the
/// `ZFS_VOL_LABEL` label, the pool from the spec, and only acts on CRs whose
/// `ownerNodeID` is its own.
pub fn target_snapshot_cr(
    target: &crate::crd::zfs_evacuation::TargetInfo,
    snap_name: &str,
    info: VolumeInfo,
) -> ZFSSnapshot {
    let info = crate::controller::adapt_to_target(info, target);
    let mut snap = ZFSSnapshot::new(snap_name, crate::crd::openebs::ZFSSnapshotSpec(info));
    // The CRD requires status on create; anything but Ready makes the agent
    // run its (idempotent) create and then mark it Ready with its finalizer.
    snap.status = Some(ZFSVolumeStatus {
        state: Some(ZFS_STATUS_PENDING.to_string()),
    });
    let labels = snap.meta_mut().labels.get_or_insert_with(Default::default);
    labels.insert("kubernetes.io/nodename".into(), target.node_id.clone());
    labels.insert(ZFS_VOL_LABEL.into(), target.new_volume_handle.clone());
    snap
}

/// Destroy the transfer snapshot the received stream recreated on the
/// target (`<new dataset>@<snap>`). zfs-localpv has no record of it, so it
/// would leak and pin every block the volume has since overwritten. Route
/// the destroy through the target agent by registering a ZFSSnapshot CR for
/// it (the agent's create is a no-op when the snapshot already exists) and
/// then deleting the CR, whose zfs finalizer runs the `zfs destroy`.
///
/// Returns `None` once the snapshot is gone, otherwise the wait message.
/// `target_snap_delete_issued` is set the moment the delete goes out so a
/// CR that is simply gone is never re-registered; a crash between the
/// delete and the status write costs one extra snapshot round trip and
/// nothing else.
pub async fn destroy_target_snapshot(
    ctx: &Ctx,
    evac_name: &str,
    st: &mut ZFSEvacuationStatus,
) -> Result<Option<String>> {
    let (Some(t), Some(target)) = (st.transfer.clone(), st.target.clone()) else {
        return Ok(None);
    };
    let snap_api: Api<ZFSSnapshot> = ctx.openebs();
    let existing = snap_api.get_opt(&t.snap_name).await?;

    if t.target_snap_delete_issued {
        return match existing {
            None => Ok(None),
            Some(_) => {
                strip_zfs_finalizer_if_node_gone(ctx, &snap_api, &t.snap_name, &target.node_id)
                    .await?;
                Ok(Some("waiting for target transfer snapshot destruction".into()))
            }
        };
    }

    match existing {
        None => {
            let zv_api: Api<ZFSVolume> = ctx.openebs();
            let Some(zv) = zv_api.get_opt(&target.new_volume_handle).await? else {
                // The new volume is already being torn down (PV released);
                // its snapshots go with the dataset.
                return Ok(None);
            };
            let snap = target_snapshot_cr(&target, &t.snap_name, zv.spec.0.clone());
            tracing::info!(evac = evac_name, snap = t.snap_name, "registering target transfer snapshot");
            create_if_absent(&snap_api, &snap).await?;
            Ok(Some("registering target transfer snapshot".into()))
        }
        Some(cr) if cr.meta().deletion_timestamp.is_some() => {
            mark_target_snap_delete_issued(ctx, evac_name, st).await?;
            Ok(Some("waiting for target transfer snapshot destruction".into()))
        }
        // The agent adds its finalizer in the same update that sets Ready;
        // deleting before that would let the CR vanish without a destroy.
        Some(cr) if cr.finalizers().iter().any(|f| f == ZFS_FINALIZER) => {
            snap_api.delete(&t.snap_name, &DeleteParams::default()).await?;
            mark_target_snap_delete_issued(ctx, evac_name, st).await?;
            Ok(Some("waiting for target transfer snapshot destruction".into()))
        }
        Some(_) => {
            // Unprocessed, no finalizer: if the node that should process it
            // is gone the dataset is too — drop the bare CR.
            if node_by_id(&ctx.nodes(), &target.node_id).await?.is_none() {
                tracing::warn!(evac = evac_name, "target node gone; dropping unprocessed snapshot CR");
                snap_api.delete(&t.snap_name, &DeleteParams::default()).await?;
                mark_target_snap_delete_issued(ctx, evac_name, st).await?;
            }
            Ok(Some("waiting for target agent to register transfer snapshot".into()))
        }
    }
}

async fn mark_target_snap_delete_issued(
    ctx: &Ctx,
    evac_name: &str,
    st: &mut ZFSEvacuationStatus,
) -> Result<()> {
    if let Some(t) = st.transfer.as_mut() {
        t.target_snap_delete_issued = true;
    }
    ctx.write_status(evac_name, st).await
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
        failure_reason: None,
        target_snap_delete_issued: false,
    });
    st.message = Some(format!("transfer attempt {attempt} starting"));
    ctx.write_status(&name, st).await?;

    // Source only. The ZFSRestore follows from `transferring` once the
    // source has connected to the relay (see there for why).
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
    Ok(Action::requeue(Duration::from_secs(2)))
}

/// Create the receive side of an attempt, pointed at the relay's restore
/// port, with the source's volSpec adapted to the target node/pool.
async fn create_restore(
    ctx: &Ctx,
    st: &ZFSEvacuationStatus,
    name: &str,
    attempt: u32,
    restore_port: u16,
) -> Result<()> {
    let source = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
    let target = st.target.clone().ok_or_else(|| anyhow!("no target recorded"))?;
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    let zv = zv_api
        .get_opt(&source.volume_handle)
        .await?
        .ok_or_else(|| anyhow!("source ZFSVolume disappeared"))?;
    let vol_spec: VolumeInfo = crate::controller::adapt_to_target(zv.spec.0.clone(), &target);

    let rst_api: Api<ZFSRestore> = ctx.openebs();
    let mut rst = ZFSRestore::new(
        &rst_name(name, attempt),
        ZFSRestoreSpec {
            volume_name: target.new_volume_handle.clone(),
            owner_node_id: target.node_id.clone(),
            restore_src: format!("{}:{}", ctx.cfg.pod_ip, restore_port),
        },
        vol_spec,
    );
    rst.status = Some(BKP_STATUS_INIT.to_string());
    tracing::info!(evac = name, attempt, "source connected; creating restore");
    create_if_absent(&rst_api, &rst).await
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

