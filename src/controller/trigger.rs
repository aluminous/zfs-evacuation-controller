//! Triggers: reconcile evacuation-worthy PVs into ZFSEvacuation CRs. All
//! state lives on the ZFSEvacuation; this loop only creates CRs.
//!
//! - Phase 1: a PV annotated `zfsevac.alumino.us/evacuate=true` — WhenIdle.
//! - Phase 2: a node tainted with the evacuate taint key — every zfs-localpv
//!   volume owned by that node is evacuated, always WhenClaimed: a volume
//!   with a consumer moves with it (the drain leaves the consumer Pending and
//!   it decides where the group goes), and a volume without one simply
//!   resolves no group and is placed on its own. Either way the node must
//!   repel pods (cordon, or the taint itself with a hard effect) before any
//!   copy starts. Both triggers lock the PVC immediately; nothing is evicted
//!   here — draining is the operator's move, made after the lock.
//!
//! A periodic list (not a watch): a pure watcher misses the "ZFSEvacuation
//! was deleted while the trigger condition remains" case — no event fires, so
//! a failed evacuation could never be retried by deleting its CR.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::PersistentVolume;
use kube::api::{Api, ListParams};
use kube::ResourceExt;

use crate::controller::{node_by_id, node_has_evacuate_taint, node_id_of, Ctx};
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
        let pv_name = pv.name_any();
        if wants_evacuation(pv)
            && clear_for_creation(ctx, &pv_name).await
            && annotation_condition_fresh(ctx, &pv_name).await
        {
            create_evacuation(ctx, &pv_name, EvacuationTrigger::Annotation, EvacuationMode::WhenIdle)
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
        if clear_for_creation(ctx, &pv_name).await
            && taint_condition_fresh(ctx, &pv_name, &zv.name_any()).await
        {
            create_evacuation(ctx, &pv_name, EvacuationTrigger::NodeTaint, EvacuationMode::WhenClaimed)
                .await;
        }
    }
    Ok(())
}

/// Fresh-read verification of the annotation condition, run between deleting
/// a Completed CR and creating its successor: the tick's PV list can predate
/// an evacuation's completion (which strips the annotation), and a create
/// from that stale view would spawn a CR whose condition never held —
/// destined to cancel into Failed and block the PV.
async fn annotation_condition_fresh(ctx: &Ctx, pv_name: &str) -> bool {
    match ctx.pvs().get_opt(pv_name).await {
        Ok(Some(pv)) => wants_evacuation(&pv),
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(pv = pv_name, error = %e, "fresh annotation check failed");
            false
        }
    }
}

/// Fresh-read verification of the taint condition: the PV must still carry
/// this volumeHandle (a completed evacuation changes it) and the volume's
/// current owner node must still carry the evacuate taint.
async fn taint_condition_fresh(ctx: &Ctx, pv_name: &str, handle: &str) -> bool {
    let fresh = async {
        let pv = ctx.pvs().get_opt(pv_name).await?;
        let handle_matches = pv
            .as_ref()
            .and_then(|pv| pv.spec.as_ref()?.csi.as_ref())
            .is_some_and(|csi| csi.volume_handle == handle);
        if !handle_matches {
            return Ok::<bool, anyhow::Error>(false);
        }
        let zv_api: Api<ZFSVolume> = ctx.openebs();
        let Some(zv) = zv_api.get_opt(handle).await? else {
            return Ok(false);
        };
        let Some(node) = node_by_id(&ctx.nodes(), &zv.spec.0.owner_node_id).await? else {
            return Ok(false);
        };
        Ok(node_has_evacuate_taint(&node, &ctx.cfg.taint_key))
    };
    match fresh.await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(pv = pv_name, error = %e, "fresh taint check failed");
            false
        }
    }
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
/// flight, true when none exists or a replaceable terminal one was just
/// removed to make room.
async fn clear_for_creation(ctx: &Ctx, pv_name: &str) -> bool {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    match api.get_opt(pv_name).await {
        Ok(Some(existing)) => {
            // A Completed CR is a finished job keeping the per-PV name; a
            // cancelled one records a withdrawn request. The trigger firing
            // again is a new request, so both are replaceable. A genuinely
            // Failed evacuation stays put — deleting it is the operator's
            // retry gesture, and it must stay visible until then.
            let replaceable = existing.status.as_ref().is_some_and(|s| {
                s.phase == Phase::Completed || (s.phase == Phase::Failed && s.cancelled)
            });
            if !replaceable {
                return false;
            }
            match api.delete(pv_name, &Default::default()).await {
                Ok(_) => {
                    tracing::info!(pv = pv_name, "replaced terminal ZFSEvacuation");
                    true
                }
                Err(kube::Error::Api(e)) if e.code == 404 => true,
                Err(e) => {
                    tracing::warn!(pv = pv_name, error = %e, "failed to delete terminal ZFSEvacuation");
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
