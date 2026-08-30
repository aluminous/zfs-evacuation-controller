//! Minimal Lease-based leader election. The relay makes concurrent
//! controllers actively harmful (two reconcilers would race transfer
//! attempts), so we block until leadership is acquired and exit the process
//! if it is ever lost — the Deployment restarts us as a follower.

use std::time::Duration;

use anyhow::{Context, Result};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Api, PostParams};
use kube::Client;

const LEASE_NAME: &str = "zfs-evacuation-controller";
const LEASE_DURATION_SECS: i32 = 30;
const RENEW_EVERY: Duration = Duration::from_secs(10);

pub async fn run(client: Client, namespace: &str, identity: String) -> Result<()> {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    // Acquire.
    loop {
        match try_acquire(&api, &identity).await {
            Ok(true) => break,
            Ok(false) => tokio::time::sleep(RENEW_EVERY).await,
            Err(e) => {
                tracing::warn!(error = %e, "leader election error");
                tokio::time::sleep(RENEW_EVERY).await;
            }
        }
    }
    tracing::info!(identity, "acquired leadership");
    // Hold: renew forever in the background; kill the process on loss. A
    // renewal that keeps ERRORING must fence too: once we can't prove we've
    // renewed within the lease duration, another replica may legitimately
    // have taken over — running on is split-brain.
    tokio::spawn(async move {
        let mut last_renewed = std::time::Instant::now();
        loop {
            tokio::time::sleep(RENEW_EVERY).await;
            match try_acquire(&api, &identity).await {
                Ok(true) => last_renewed = std::time::Instant::now(),
                Ok(false) => {
                    tracing::error!("lost leadership; exiting");
                    std::process::exit(1);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "lease renew error (will retry)");
                    if last_renewed.elapsed().as_secs() > LEASE_DURATION_SECS as u64 {
                        tracing::error!("could not renew lease within its duration; fencing (exit)");
                        std::process::exit(1);
                    }
                }
            }
        }
    });
    Ok(())
}

fn now() -> MicroTime {
    MicroTime(k8s_openapi::jiff::Timestamp::now())
}

async fn try_acquire(api: &Api<Lease>, identity: &str) -> Result<bool> {
    match api.get_opt(LEASE_NAME).await? {
        None => {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(LEASE_NAME.into()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(identity.into()),
                    lease_duration_seconds: Some(LEASE_DURATION_SECS),
                    acquire_time: Some(now()),
                    renew_time: Some(now()),
                    lease_transitions: Some(0),
                    ..Default::default()
                }),
            };
            match api.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e).context("creating lease"),
            }
        }
        Some(lease) => {
            let spec = lease.spec.clone().unwrap_or_default();
            let holder = spec.holder_identity.clone().unwrap_or_default();
            let expired = spec
                .renew_time
                .as_ref()
                .map(|t| {
                    k8s_openapi::jiff::Timestamp::now().as_second() - t.0.as_second()
                        > spec.lease_duration_seconds.unwrap_or(LEASE_DURATION_SECS) as i64
                })
                .unwrap_or(true);
            if holder != identity && !expired {
                return Ok(false);
            }
            // Replace with the observed resourceVersion so a concurrent
            // takeover conflicts instead of split-braining.
            let mut updated = lease.clone();
            let s = updated.spec.get_or_insert_with(Default::default);
            if holder != identity {
                s.lease_transitions = Some(spec.lease_transitions.unwrap_or(0) + 1);
                s.acquire_time = Some(now());
            }
            s.holder_identity = Some(identity.into());
            s.lease_duration_seconds = Some(LEASE_DURATION_SECS);
            s.renew_time = Some(now());
            match api.replace(LEASE_NAME, &PostParams::default(), &updated).await {
                Ok(_) => Ok(true),
                // Conflict while renewing our own lease is transient; while
                // taking over it means someone else won.
                Err(kube::Error::Api(e)) if e.code == 409 && holder == identity => {
                    Err(anyhow::anyhow!("conflict renewing own lease"))
                }
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(e).context("renewing lease"),
            }
        }
    }
}
