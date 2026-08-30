//! Phase-1 trigger: reconcile PVs carrying the evacuate annotation into
//! ZFSEvacuation CRs. All state lives on the ZFSEvacuation; this loop is
//! intentionally dumb (phase 2 adds a node-taint trigger that fans out to the
//! same CRs).
//!
//! A periodic list (not a watch): a pure watcher misses the "ZFSEvacuation
//! was deleted while the annotation remains" case — no PV event fires, so a
//! failed evacuation could never be retried by deleting its CR.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::PersistentVolume;
use kube::api::{Api, ListParams};
use kube::ResourceExt;

use crate::crd::openebs::ZFS_DRIVER;
use crate::crd::zfs_evacuation::{ZFSEvacuation, ZFSEvacuationSpec, EVACUATE_ANNOTATION};
use crate::controller::Ctx;

const POLL_INTERVAL: Duration = Duration::from_secs(15);

pub async fn run(ctx: Arc<Ctx>) -> anyhow::Result<()> {
    let pvs: Api<PersistentVolume> = Api::all(ctx.client.clone());
    loop {
        match pvs.list(&ListParams::default()).await {
            Ok(list) => {
                for pv in &list {
                    if wants_evacuation(pv)
                        && let Err(e) = ensure_evacuation(&ctx, pv).await {
                            tracing::warn!(pv = pv.name_any(), error = %e, "failed to create ZFSEvacuation");
                        }
                }
            }
            Err(e) => tracing::warn!(error = %e, "PV trigger list failed"),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
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

async fn ensure_evacuation(ctx: &Ctx, pv: &PersistentVolume) -> anyhow::Result<()> {
    let api: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let name = pv.name_any();
    if api.get_opt(&name).await?.is_some() {
        return Ok(());
    }
    let evac = ZFSEvacuation::new(
        &name,
        ZFSEvacuationSpec {
            pv_name: name.clone(),
            target_node: None,
            target_pool: None,
            settle_seconds: None,
            transfer_timeout_seconds: None,
            max_attempts: None,
            headroom_percent: None,
        },
    );
    match api.create(&Default::default(), &evac).await {
        Ok(_) => {
            tracing::info!(pv = name, "created ZFSEvacuation");
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}
