//! Abort path: unwind everything the evacuation did, in reverse, leaving the
//! source volume exactly as it was. Only reachable before the commit point —
//! after the old PV delete is issued, the machine rolls forward only.

use std::time::Duration;

use anyhow::{anyhow, Result};
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};
use serde_json::json;

use crate::controller::state_machine::{
    cleanup_transfer_crs, create_if_absent, remove_evac_finalizer, rst_name, set_finalizers,
    strip_zfs_finalizer_if_node_gone,
};
use crate::controller::{pv_swap, Ctx};
use crate::crd::openebs::{
    ZFSRestore, ZFSVolume, ZFSVolumeSpec, ZFSVolumeStatus, BKP_STATUS_DONE, ZFS_STATUS_PENDING,
};
use crate::crd::zfs_evacuation::{
    Phase, ZFSEvacuation, ZFSEvacuationStatus, EVACUATING_LABEL, GUARD_FINALIZER,
};

pub async fn run(
    ctx: &Ctx,
    evac: &ZFSEvacuation,
    st: &mut ZFSEvacuationStatus,
) -> Result<Action> {
    let name = evac.name_any();
    if st.phase != Phase::Aborting {
        st.phase = Phase::Aborting;
        ctx.write_status(&name, st).await?;
    }

    // 1. Stop any live relay.
    if let Some(t) = ctx.transfers.lock().unwrap().remove(&name) {
        t.relay.task.abort();
    }

    // 2. Target-side dataset cleanup, using the ZFSRestore CR's Done status as
    //    the marker that a full dataset landed. Ordering matters: this must
    //    happen while the transfer CRs still exist, so re-entry is idempotent.
    if let (Some(target), Some(t)) = (st.target.clone(), st.transfer.clone()) {
        let zv_api: Api<ZFSVolume> = ctx.openebs();
        let rst_api: Api<ZFSRestore> = ctx.openebs();
        let rst = rst_api.get_opt(&rst_name(&name, t.attempt)).await?;
        let restore_done = rst
            .as_ref()
            .and_then(|r| r.status.as_deref())
            .map(|s| s == BKP_STATUS_DONE)
            .unwrap_or(false);

        match zv_api.get_opt(&target.new_volume_handle).await? {
            Some(zv) => {
                // Delete it; the target agent destroys the received dataset.
                if zv.meta().deletion_timestamp.is_none() {
                    zv_api
                        .delete(&target.new_volume_handle, &DeleteParams::default())
                        .await?;
                }
                strip_zfs_finalizer_if_node_gone(
                    ctx,
                    &zv_api,
                    &target.new_volume_handle,
                    &target.node_id,
                )
                .await?;
                return requeue_msg(ctx, &name, st, "abort: destroying target dataset").await;
            }
            None if restore_done => {
                // A dataset landed but no ZFSVolume was ever created for it:
                // create-then-delete so the agent adopts and destroys it.
                let src = st.source.clone().ok_or_else(|| anyhow!("no source recorded"))?;
                if let Some(src_zv) = zv_api.get_opt(&src.volume_handle).await? {
                    let mut info = src_zv.spec.0.clone();
                    info.owner_node_id = target.node_id.clone();
                    info.pool_name = target.pool.clone();
                    info.snapname = None;
                    let mut zv =
                        ZFSVolume::new(&target.new_volume_handle, ZFSVolumeSpec(info));
                    zv.meta_mut()
                        .labels
                        .get_or_insert_with(Default::default)
                        .insert("kubernetes.io/nodename".into(), target.node_id.clone());
                    zv.status = Some(ZFSVolumeStatus {
                        state: Some(ZFS_STATUS_PENDING.to_string()),
                    });
                    create_if_absent(&zv_api, &zv).await?;
                    return requeue_msg(ctx, &name, st, "abort: adopting orphan dataset for destruction")
                        .await;
                }
            }
            None => {}
        }

        // 3. Transfer CRs (also drops the source-side transfer snapshot).
        if !cleanup_transfer_crs(ctx, &name, t.attempt, st).await? {
            return requeue_msg(ctx, &name, st, "abort: cleaning up transfer CRs").await;
        }
    }

    // 4. Restore the original reclaim policy on the (still-original) PV.
    if let (Some(source), Some(orig)) = (st.source.clone(), st.original_reclaim_policy.clone())
        && let Some(pv) = ctx.pvs().get_opt(&evac.spec.pv_name).await?
            && pv.uid().as_deref() == Some(source.pv_uid.as_str()) {
                ctx.pvs()
                    .patch(
                        &evac.spec.pv_name,
                        &PatchParams::default(),
                        &Patch::Merge(
                            json!({"spec": {"persistentVolumeReclaimPolicy": orig}}),
                        ),
                    )
                    .await?;
            }

    // 5. Unlock.
    if let Some(pvc_ref) = &st.pvc_ref {
        let key = format!("{}/{}", pvc_ref.namespace, pvc_ref.name);
        ctx.update_params(|keys| {
            let before = keys.len();
            keys.retain(|k| k != &key);
            keys.len() != before
        })
        .await?;
        let _ = ctx
            .pvcs(&pvc_ref.namespace)
            .patch(
                &pvc_ref.name,
                &PatchParams::default(),
                &Patch::Merge(json!({"metadata": {"labels": {EVACUATING_LABEL: null}}})),
            )
            .await;
    }

    // 6. Honor a deletion the user requested mid-evacuation: if the PVC is
    //    gone, the PV is gone, and the volume would have been reclaimed with
    //    Delete, nothing remains to drive the reclaim — do it ourselves.
    if let Some(source) = st.source.clone() {
        let zv_api: Api<ZFSVolume> = ctx.openebs();
        let pvc_gone = match &st.pvc_ref {
            Some(r) => ctx.pvcs(&r.namespace).get_opt(&r.name).await?.is_none(),
            None => false,
        };
        let pv_gone = ctx.pvs().get_opt(&evac.spec.pv_name).await?.is_none();
        if pvc_gone && pv_gone && st.original_reclaim_policy.as_deref() == Some("Delete")
            && let Some(zv) = zv_api.get_opt(&source.volume_handle).await?
                && zv.meta().deletion_timestamp.is_none() {
                    zv_api
                        .delete(&source.volume_handle, &DeleteParams::default())
                        .await?;
                }
        // 7. Drop the guard last: whatever deletion machinery applies (ours or
        //    the external-provisioner's) may now actually destroy the dataset.
        set_finalizers(&zv_api, &source.volume_handle, |cur| {
            cur.iter().filter(|f| *f != GUARD_FINALIZER).cloned().collect()
        })
        .await?;
        strip_zfs_finalizer_if_node_gone(ctx, &zv_api, &source.volume_handle, &source.node_id)
            .await?;
    }

    pv_swap::delete_recovery_configmap(ctx, &name).await?;

    let msg = st
        .message
        .clone()
        .unwrap_or_else(|| "evacuation aborted".to_string());
    if evac.meta().deletion_timestamp.is_some() {
        remove_evac_finalizer(ctx, &name).await?;
        return Ok(Action::await_change());
    }
    st.phase = Phase::Failed;
    st.message = Some(msg);
    ctx.write_status(&name, st).await?;
    Ok(Action::await_change())
}

async fn requeue_msg(
    ctx: &Ctx,
    name: &str,
    st: &mut ZFSEvacuationStatus,
    msg: &str,
) -> Result<Action> {
    if st.message.as_deref() != Some(msg) {
        st.message = Some(msg.to_string());
        ctx.write_status(name, st).await?;
    }
    Ok(Action::requeue(Duration::from_secs(5)))
}
