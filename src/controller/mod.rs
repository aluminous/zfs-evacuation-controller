pub mod abort;
pub mod pv_swap;
pub mod state_machine;
pub mod target;
pub mod trigger;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;

use anyhow::{anyhow, Context as _, Result};
use k8s_openapi::api::core::v1::{Node, PersistentVolume, PersistentVolumeClaim, Pod};
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::{Client, ResourceExt};
use serde_json::json;

use crate::crd::openebs::NODE_ID_TOPOLOGY_KEY;
use crate::crd::zfs_evacuation::{
    EvacuationParams, EvacuationParamsSpec, ZFSEvacuation, ZFSEvacuationStatus, PARAMS_NAME,
};
use crate::transfer::relay::Relay;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error("{0:#}")]
    Other(#[from] anyhow::Error),
}

/// An in-flight transfer attempt's relay, keyed by evacuation name. A missing
/// entry for a status-recorded attempt means the controller restarted and the
/// attempt must be invalidated.
pub struct ActiveTransfer {
    pub attempt: u32,
    pub relay: Relay,
}

pub struct Config {
    /// Namespace the zfs-localpv CRs live in (OPENEBS_NAMESPACE).
    pub openebs_ns: String,
    /// Our pod IP — advertised to the node agents as the relay address.
    pub pod_ip: String,
    /// Namespace we run in (recovery ConfigMaps live here).
    pub pod_namespace: String,
    /// Seconds without relay progress before an attempt is declared stalled.
    pub stall_seconds: u64,
}

pub struct Ctx {
    pub client: Client,
    pub cfg: Config,
    pub transfers: Mutex<HashMap<String, ActiveTransfer>>,
    /// Serializes TargetSelecting's check-then-act (capacity reservation +
    /// concurrency caps) across concurrent reconciles. Process-local is
    /// sufficient: the controller is single-leader.
    pub selection_lock: tokio::sync::Mutex<()>,
}

pub const DEFAULT_SETTLE_SECONDS: u64 = 90;
pub const DEFAULT_TRANSFER_TIMEOUT: u64 = 3600;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
pub const DEFAULT_HEADROOM_PERCENT: u32 = 10;
/// Grace after updating the VAP param object before trusting the lock.
pub const LOCK_PROPAGATION_SECONDS: u64 = 5;

impl Ctx {
    pub fn pvs(&self) -> Api<PersistentVolume> {
        Api::all(self.client.clone())
    }
    pub fn nodes(&self) -> Api<Node> {
        Api::all(self.client.clone())
    }
    pub fn pvcs(&self, ns: &str) -> Api<PersistentVolumeClaim> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn pods(&self, ns: &str) -> Api<Pod> {
        Api::namespaced(self.client.clone(), ns)
    }
    pub fn openebs<K>(&self) -> Api<K>
    where
        K: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>
            + serde::de::DeserializeOwned
            + std::fmt::Debug
            + Clone,
        K::DynamicType: Default,
    {
        Api::namespaced(self.client.clone(), &self.cfg.openebs_ns)
    }

    /// Replace the whole controller-owned status of an evacuation.
    pub async fn write_status(&self, name: &str, status: &ZFSEvacuationStatus) -> Result<()> {
        let api: Api<ZFSEvacuation> = Api::all(self.client.clone());
        api.patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await
        .with_context(|| format!("writing status of ZFSEvacuation {name}"))?;
        Ok(())
    }

    /// Read-modify-write the singleton EvacuationParams (VAP param object)
    /// with conflict retries. `mutate` returns true if a write is needed.
    pub async fn update_params(&self, mutate: impl Fn(&mut Vec<String>) -> bool) -> Result<()> {
        let api: Api<EvacuationParams> = Api::all(self.client.clone());
        for _ in 0..8 {
            match api.get_opt(PARAMS_NAME).await? {
                None => {
                    let mut keys = Vec::new();
                    if !mutate(&mut keys) {
                        return Ok(());
                    }
                    let params = EvacuationParams::new(
                        PARAMS_NAME,
                        EvacuationParamsSpec { pvc_keys: keys },
                    );
                    match api.create(&Default::default(), &params).await {
                        Ok(_) => return Ok(()),
                        Err(kube::Error::Api(e)) if e.code == 409 => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                Some(mut params) => {
                    if !mutate(&mut params.spec.pvc_keys) {
                        return Ok(());
                    }
                    let name = params.name_any();
                    match api.replace(&name, &Default::default(), &params).await {
                        Ok(_) => return Ok(()),
                        Err(kube::Error::Api(e)) if e.code == 409 => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Err(anyhow!("persistent conflict updating {PARAMS_NAME}"))
    }
}

/// Node identity as zfs-localpv sees it: the `openebs.io/nodeid` label if
/// present, else the node name.
pub fn node_id_of(node: &Node) -> String {
    node.labels()
        .get(NODE_ID_TOPOLOGY_KEY)
        .cloned()
        .unwrap_or_else(|| node.name_any())
}

/// Find the k8s Node whose zfs-localpv identity matches `node_id`.
pub async fn node_by_id(nodes: &Api<Node>, node_id: &str) -> Result<Option<Node>> {
    if let Some(n) = nodes.get_opt(node_id).await? {
        // A node named like the id but overridden by a label is NOT a match.
        if node_id_of(&n) == node_id {
            return Ok(Some(n));
        }
    }
    let all = nodes.list(&ListParams::default()).await?;
    Ok(all.into_iter().find(|n| node_id_of(n) == node_id))
}

pub fn node_ips(node: &Node) -> Vec<IpAddr> {
    node.status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .map(|addrs| {
            addrs
                .iter()
                .filter(|a| a.type_ == "InternalIP" || a.type_ == "ExternalIP")
                .filter_map(|a| a.address.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

pub fn node_is_ready(node: &Node) -> bool {
    let ready = node
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
        .unwrap_or(false);
    let schedulable = !node.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false);
    ready && schedulable
}

/// Does this pod reference the PVC — directly, or via a generic ephemeral
/// volume (derived PVC name `<pod>-<volume>`)?
pub fn pod_references_pvc(pod: &Pod, pvc_name: &str) -> bool {
    let pod_name = pod.name_any();
    pod.spec
        .as_ref()
        .and_then(|s| s.volumes.as_ref())
        .map(|volumes| {
            volumes.iter().any(|v| {
                v.persistent_volume_claim
                    .as_ref()
                    .map(|c| c.claim_name == pvc_name)
                    .unwrap_or(false)
                    || (v.ephemeral.is_some() && format!("{pod_name}-{}", v.name) == pvc_name)
            })
        })
        .unwrap_or(false)
}

/// Pods that reference the PVC. Terminating and completed pods count: a pod
/// object's existence is our only unmount signal.
pub async fn pods_referencing_pvc(pods: &Api<Pod>, pvc_name: &str) -> Result<Vec<String>> {
    let list = pods.list(&ListParams::default()).await?;
    Ok(list
        .into_iter()
        .filter(|p| pod_references_pvc(p, pvc_name))
        .map(|p| p.name_any())
        .collect())
}

/// Parse a Kubernetes resource.Quantity into bytes (decimal + binary suffixes).
pub fn parse_quantity(q: &str) -> Option<u128> {
    let q = q.trim();
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+'))?;
    let (num, suffix) = if split == 0 {
        return None;
    } else {
        q.split_at(split)
    };
    let mult: u128 = match suffix {
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        _ => return None,
    };
    let val: f64 = num.parse().ok()?;
    if val < 0.0 {
        return None;
    }
    Some((val * mult as f64) as u128)
}

/// Parse a quantity that may also be a plain byte count (no suffix).
pub fn parse_quantity_or_bytes(q: &str) -> Option<u128> {
    q.trim().parse::<u128>().ok().or_else(|| parse_quantity(q))
}

pub fn now_rfc3339() -> String {
    humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string()
}

pub fn secs_since(rfc3339: &str) -> Option<u64> {
    let t = humantime::parse_rfc3339(rfc3339).ok()?;
    std::time::SystemTime::now().duration_since(t).ok().map(|d| d.as_secs())
}
