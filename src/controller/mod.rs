pub mod abort;
pub mod colocation;
pub mod placement;
pub mod pv_swap;
pub mod state_machine;
pub mod transfer;
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
    /// Namespace we run in (leader-election Lease lives here).
    pub pod_namespace: String,
    /// Seconds without relay progress before an attempt is declared stalled.
    pub stall_seconds: u64,
    /// Taint key that marks a node for evacuation (phase-2 trigger). A node
    /// carrying this taint (any effect) has all its zfs-localpv volumes
    /// evacuated as they become unused, and is excluded as a target.
    pub taint_key: String,
}

/// zfs-localpv addresses a CR to a node through three things at once: the
/// spec's ownerNodeID/poolName (with no clone origin), the
/// `kubernetes.io/nodename` label its informers select on, and a non-Ready
/// status that makes the agent run its (idempotent) create. Every
/// target-side CR — ZFSVolume, ZFSSnapshot, a restore's volSpec — must agree
/// on the spec part, and the constructors below on all three; this is the
/// single place that knowledge lives.
pub fn adapt_to_target(
    mut info: crate::crd::openebs::VolumeInfo,
    target: &crate::crd::zfs_evacuation::TargetInfo,
) -> crate::crd::openebs::VolumeInfo {
    info.owner_node_id = target.node_id.clone();
    info.pool_name = target.pool.clone();
    info.snapname = None;
    info
}

/// The ZFSVolume CR for the received dataset on the target (used both to
/// adopt it and, on abort, to route its destruction through the agent).
pub fn target_zfsvolume(
    info: crate::crd::openebs::VolumeInfo,
    target: &crate::crd::zfs_evacuation::TargetInfo,
) -> crate::crd::openebs::ZFSVolume {
    use crate::crd::openebs::{ZFSVolume, ZFSVolumeSpec, ZFSVolumeStatus, ZFS_STATUS_PENDING};
    use kube::Resource as _;
    let mut zv = ZFSVolume::new(
        &target.new_volume_handle,
        ZFSVolumeSpec(adapt_to_target(info, target)),
    );
    zv.meta_mut()
        .labels
        .get_or_insert_with(Default::default)
        .insert("kubernetes.io/nodename".into(), target.node_id.clone());
    zv.status = Some(ZFSVolumeStatus {
        state: Some(ZFS_STATUS_PENDING.to_string()),
    });
    zv
}

/// Does the node carry the evacuate taint (any effect)?
pub fn node_has_evacuate_taint(node: &Node, taint_key: &str) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|ts| ts.iter().any(|t| t.key == taint_key))
        .unwrap_or(false)
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
    pub async fn update_params(
        &self,
        mutate: impl Fn(&mut EvacuationParamsSpec) -> bool,
    ) -> Result<()> {
        let api: Api<EvacuationParams> = Api::all(self.client.clone());
        for _ in 0..8 {
            match api.get_opt(PARAMS_NAME).await? {
                None => {
                    let mut spec = EvacuationParamsSpec::default();
                    if !mutate(&mut spec) {
                        return Ok(());
                    }
                    let params = EvacuationParams::new(PARAMS_NAME, spec);
                    match api.create(&Default::default(), &params).await {
                        Ok(_) => return Ok(()),
                        Err(kube::Error::Api(e)) if e.code == 409 => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                Some(mut params) => {
                    if !mutate(&mut params.spec) {
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

/// Will the scheduler keep ordinary pods off this node? True when it is
/// cordoned (`spec.unschedulable`) or carries the evacuate taint with a hard
/// effect. This is what stands in for the attach lock under WhenClaimed: the
/// consumer's replacement pod exists, and only the node's own state keeps it
/// from landing back on the source mid-copy.
pub fn node_repels_pods(node: &Node, taint_key: &str) -> bool {
    let Some(spec) = node.spec.as_ref() else {
        return false;
    };
    spec.unschedulable.unwrap_or(false)
        || spec.taints.as_ref().is_some_and(|ts| {
            ts.iter()
                .any(|t| t.key == taint_key && (t.effect == "NoSchedule" || t.effect == "NoExecute"))
        })
}

/// PVC names a pod references: plain claims, and generic ephemeral volumes
/// under their derived name `<pod>-<volume>`.
pub fn pod_pvc_names(pod: &Pod) -> Vec<String> {
    let pod_name = pod.name_any();
    pod.spec
        .as_ref()
        .and_then(|s| s.volumes.as_ref())
        .map(|volumes| {
            volumes
                .iter()
                .filter_map(|v| {
                    if let Some(c) = &v.persistent_volume_claim {
                        Some(c.claim_name.clone())
                    } else {
                        v.ephemeral.as_ref().map(|_| format!("{pod_name}-{}", v.name))
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Does this pod reference the PVC — directly, or via a generic ephemeral
/// volume (derived PVC name `<pod>-<volume>`)?
pub fn pod_references_pvc(pod: &Pod, pvc_name: &str) -> bool {
    pod_pvc_names(pod).iter().any(|n| n == pvc_name)
}

/// Pods that reference the PVC. Terminating and completed pods count: a pod
/// object's existence is our only unmount signal.
pub async fn pods_referencing_pvc(pods: &Api<Pod>, pvc_name: &str) -> Result<Vec<Pod>> {
    let list = pods.list(&ListParams::default()).await?;
    Ok(list
        .into_iter()
        .filter(|p| pod_references_pvc(p, pvc_name))
        .collect())
}

/// Has the scheduler bound this pod to a node? A never-scheduled pod holds
/// no attachment. Under WhenIdle it still blocks Quiescing (strict zero-pods
/// rule; the status message says which kind is in the way); under
/// WhenClaimed it is the consumer waiting for the volume to arrive.
pub fn pod_is_scheduled(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .map(|n| !n.is_empty())
        .unwrap_or(false)
}

/// Never-scheduled pods, name-sorted so every reconcile picks the same
/// consumer when several exist.
pub fn unscheduled_pods(pods: &[Pod]) -> Vec<&Pod> {
    let mut v: Vec<&Pod> = pods.iter().filter(|p| !pod_is_scheduled(p)).collect();
    v.sort_by_key(|p| p.name_any());
    v
}

pub fn pod_names(pods: &[Pod]) -> Vec<String> {
    pods.iter().map(|p| p.name_any()).collect()
}

/// Status text for pods blocking a locked PVC. Never-scheduled pods are
/// called out separately: they predate the lock (the VAP only sees creation),
/// and deleting them is the fix — the armed lock denies their recreation.
pub fn blocking_pods_message(pods: &[Pod]) -> String {
    let (scheduled, unscheduled): (Vec<&Pod>, Vec<&Pod>) =
        pods.iter().partition(|p| pod_is_scheduled(p));
    let names = |v: &[&Pod]| v.iter().map(|p| p.name_any()).collect::<Vec<_>>().join(", ");
    let mut parts = Vec::new();
    if !scheduled.is_empty() {
        parts.push(format!("waiting for pods to release PVC: {}", names(&scheduled)));
    }
    if !unscheduled.is_empty() {
        parts.push(format!(
            "never-scheduled pods predate the lock and hold it open: {} \
             (delete them; the attach-lock denies recreation until the volume has moved)",
            names(&unscheduled)
        ));
    }
    parts.join("; ")
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
