mod controller;
mod crd;
mod leader;
#[cfg(test)]
mod tests;
mod transfer;

use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use kube::api::Api;
use kube::runtime::{controller::Controller, watcher};
use kube::{Client, CustomResourceExt};

use crate::controller::{state_machine, trigger, Config, Ctx};
use crate::crd::zfs_evacuation::{EvacuationParams, ZFSEvacuation};

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().any(|a| a == "--print-crds") {
        print!("{}", serde_yaml::to_string(&ZFSEvacuation::crd())?);
        println!("---");
        print!("{}", serde_yaml::to_string(&EvacuationParams::crd())?);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kube=warn".into()),
        )
        .init();

    let cfg = Config {
        openebs_ns: std::env::var("OPENEBS_NAMESPACE").unwrap_or_else(|_| "openebs".into()),
        pod_ip: std::env::var("POD_IP").context("POD_IP must be set (downward API)")?,
        pod_namespace: std::env::var("POD_NAMESPACE")
            .unwrap_or_else(|_| "zfs-evacuation-system".into()),
        stall_seconds: std::env::var("TRANSFER_STALL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120),
        taint_key: std::env::var("EVACUATE_TAINT_KEY")
            .unwrap_or_else(|_| "zfsevac.alumino.us/evacuate".into()),
    };

    let client = Client::try_default().await?;
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("zfs-evac-{}", std::process::id()));
    leader::run(client.clone(), &cfg.pod_namespace.clone(), identity).await?;

    let ctx = Arc::new(Ctx {
        client: client.clone(),
        cfg,
        transfers: Default::default(),
        selection_lock: Default::default(),
    });

    // Phase-1 trigger: PV annotation watcher.
    {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = trigger::run(ctx.clone()).await {
                    tracing::error!(error = %e, "PV trigger watcher failed; restarting");
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    let evacs: Api<ZFSEvacuation> = Api::all(client);
    tracing::info!("starting evacuation reconciler");
    Controller::new(evacs, watcher::Config::default())
        .shutdown_on_signal()
        .run(state_machine::reconcile, state_machine::error_policy, ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "reconcile dispatch error");
            }
        })
        .await;
    Ok(())
}
