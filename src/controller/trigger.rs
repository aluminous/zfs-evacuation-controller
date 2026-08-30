//! Triggers: reconcile evacuation-worthy PVs into ZFSEvacuation CRs. All
//! state lives on the ZFSEvacuation; this loop only creates CRs.
//!
//! - Phase 1: a PV annotated `zfsevac.alumino.us/evacuate=true`.
//! - Phase 2: a node tainted with the evacuate taint key — every zfs-localpv
//!   volume owned by that node is evacuated (each one waiting, as always, for
//!   its PVC to be unused; running pods are never disturbed or evicted).
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

use crate::controller::{node_has_evacuate_taint, node_id_of, Ctx};
use crate::crd::openebs::{ZFSVolume, ZFS_DRIVER};
use crate::crd::zfs_evacuation::{
    EvacuationTrigger, ZFSEvacuation, ZFSEvacuationSpec, EVACUATE_ANNOTATION,
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
        if wants_evacuation(pv) {
            ensure_evacuation(ctx, &pv.name_any(), EvacuationTrigger::Annotation).await;
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
    let handle_to_pv: HashMap<String, String> = pv_list
        .iter()
        .filter_map(|pv| {
            let csi = pv.spec.as_ref()?.csi.as_ref()?;
            (csi.driver == ZFS_DRIVER).then(|| (csi.volume_handle.clone(), pv.name_any()))
        })
        .collect();
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    for zv in zv_api.list(&ListParams::default()).await? {
        if !tainted_ids.contains(&zv.spec.0.owner_node_id) {
            continue;
        }
        let Some(pv_name) = handle_to_pv.get(&zv.name_any()) else {
            // Dataset with no PV (mid-migration artifacts, statically managed
            // volumes) — nothing for us to move.
            continue;
        };
        ensure_evacuation(ctx, pv_name, EvacuationTrigger::NodeTaint).await;
    }
    Ok(())
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

async fn ensure_evacuation(ctx: &Ctx, pv_name: &str, trigger: EvacuationTrigger) {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    match api.get_opt(pv_name).await {
        Ok(Some(_)) => {} // exists (any phase) — deleting it is the retry gesture
        Ok(None) => {
            let evac = ZFSEvacuation::new(
                pv_name,
                ZFSEvacuationSpec {
                    pv_name: pv_name.to_string(),
                    trigger: trigger.clone(),
                    target_node: None,
                    target_pool: None,
                    settle_seconds: None,
                    transfer_timeout_seconds: None,
                    max_attempts: None,
                    headroom_percent: None,
                },
            );
            match api.create(&Default::default(), &evac).await {
                Ok(_) => tracing::info!(pv = pv_name, ?trigger, "created ZFSEvacuation"),
                Err(kube::Error::Api(e)) if e.code == 409 => {}
                Err(e) => {
                    tracing::warn!(pv = pv_name, error = %e, "failed to create ZFSEvacuation")
                }
            }
        }
        Err(e) => tracing::warn!(pv = pv_name, error = %e, "failed to check ZFSEvacuation"),
    }
}
