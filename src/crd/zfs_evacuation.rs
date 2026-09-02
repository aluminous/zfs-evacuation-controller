//! The controller's own CRDs: ZFSEvacuation (one per in-flight evacuation,
//! cluster-scoped, named after the PV) and EvacuationParams (the single
//! ValidatingAdmissionPolicy param object listing PVC keys under evacuation).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// PV annotation that triggers an evacuation ("true" = evacuate when unused).
/// Removing it before the commit point cancels the evacuation.
pub const EVACUATE_ANNOTATION: &str = "zfsevac.alumino.us/evacuate";
/// Our user-finalizer placed on the source ZFSVolume: the zfs-localpv node
/// agent will not `zfs destroy` while it is present.
pub const GUARD_FINALIZER: &str = "zfsevac.alumino.us/guard";
/// Finalizer on ZFSEvacuation so deletion runs abort logic.
pub const EVACUATION_FINALIZER: &str = "zfsevac.alumino.us/cleanup";
/// Human-visible marker label put on the PVC while evacuating (the actual
/// attach lock is the VAP param object, not this label).
pub const EVACUATING_LABEL: &str = "zfsevac.alumino.us/evacuating";
/// Fixed name of the singleton EvacuationParams object the VAP binding references.
pub const PARAMS_NAME: &str = "zfs-evacuation-locks";

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "zfsevac.alumino.us",
    version = "v1alpha1",
    kind = "ZFSEvacuation",
    plural = "zfsevacuations",
    shortname = "zevac",
    status = "ZFSEvacuationStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Mode","type":"string","jsonPath":".spec.mode"}"#,
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".status.target.node"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ZFSEvacuationSpec {
    /// Name of the PV to evacuate (immutable; also the ZFSEvacuation name).
    pub pv_name: String,
    /// What created this evacuation — governs the cancel condition:
    /// Annotation evacuations cancel when the PV annotation is removed,
    /// NodeTaint evacuations cancel when the source node's taint is removed.
    #[serde(default)]
    pub trigger: EvacuationTrigger,
    /// How the consumer gets its volume back; read once, in Locking. The
    /// NodeTaint trigger sets WhenClaimed for volumes a pod references at
    /// trigger time and WhenIdle for the rest; Annotation sets WhenIdle.
    #[serde(default)]
    pub mode: EvacuationMode,
    /// Optional explicit target node (else auto-selected). Under WhenClaimed
    /// it also anchors the consumer's other volumes: their evacuations
    /// follow it instead of choosing for themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node: Option<String>,
    /// Optional explicit destination poolname, verbatim — may be a dataset
    /// path ("zroot/csi"). Default: the PV's StorageClass `poolname`
    /// parameter, else the source volume's poolName carried over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_pool: Option<String>,
    /// Seconds to wait after the last pod disappears before snapshotting
    /// (kubelet unmount lag). Default 90.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle_seconds: Option<u64>,
    /// Per-attempt transfer timeout in seconds. Default 3600.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_timeout_seconds: Option<u64>,
    /// Maximum transfer attempts. Default 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// Extra free-space headroom required on the target pool, percent. Default 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headroom_percent: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum EvacuationTrigger {
    #[default]
    Annotation,
    NodeTaint,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum EvacuationMode {
    /// The attach lock denies every pod creation referencing the PVC; the
    /// volume moves once no pod object references it, and the workload's
    /// replacement is created (and scheduled) only after the swap. Works
    /// without a cordon, so it suits single-volume and emergency moves, and
    /// places each volume on its own.
    #[default]
    WhenIdle,
    /// The consumer's replacement pod is wanted: created normally, it stays
    /// Pending because the source is cordoned, defines the group of volumes
    /// to keep together and the placement they must satisfy, and binds the
    /// relocated volume by itself after the swap (no delete, no recreate).
    /// The attach lock denies only pods that bypass the cordon (spec.nodeName,
    /// or a toleration for node.kubernetes.io/unschedulable). Quiescing
    /// refuses to proceed with a never-scheduled consumer while the source
    /// node is schedulable.
    WhenClaimed,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Pending,
    Guarding,
    Retaining,
    Locking,
    Quiescing,
    TargetSelecting,
    Transferring,
    Adopting,
    Committing,
    Swapping,
    CleaningUp,
    Completed,
    Aborting,
    Failed,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcRef {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    pub node: String,
    /// Resolved openebs.io/nodeid (label value, or node name).
    pub node_id: String,
    pub pool: String,
    pub volume_handle: String,
    pub pv_uid: String,
    /// Volume size in bytes (for target capacity reservations).
    #[serde(default)]
    pub capacity_bytes: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetInfo {
    pub node: String,
    pub node_id: String,
    /// Full destination poolname (may be a dataset path): becomes the new
    /// ZFSVolume's poolName. Capacity accounting uses its zpool component.
    pub pool: String,
    pub new_volume_handle: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransferStatus {
    pub attempt: u32,
    pub snap_name: String,
    /// [backup_port, restore_port] the relay listens on.
    pub ports: Vec<u16>,
    #[serde(default)]
    pub bytes_relayed: Option<u64>,
    #[serde(default)]
    pub started_at: Option<String>,
    /// Set the moment an attempt is judged failed; the attempt's CRs are
    /// then torn down across several reconciles and this keeps the verdict
    /// from being re-derived (wrongly) from the torn-down state.
    #[serde(default)]
    pub failure_reason: Option<String>,
    /// CleaningUp has issued the delete of the ZFSSnapshot CR that stands in
    /// for the received copy of the transfer snapshot on the target; a later
    /// reconcile that finds no CR must not register it again.
    #[serde(default)]
    pub target_snap_delete_issued: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum ColocationRole {
    /// Chose the target for the whole group and reserved its capacity there.
    #[default]
    Leader,
    /// Took the node a sibling already has (or is heading to).
    Follower,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ColocationMember {
    pub pv_name: String,
    #[serde(default)]
    pub capacity_bytes: u64,
}

/// WhenClaimed only: the never-scheduled consumer whose volumes are kept
/// together, and how this evacuation's target was derived from it.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColocationStatus {
    /// "namespace/name" of the Pending pod that defined the group.
    pub pod: String,
    /// The group's zfs-localpv volumes still to be moved off the source
    /// (this one included). A Leader reserves their summed capacity on its
    /// target until each member's own evacuation has picked a target.
    #[serde(default)]
    pub members: Vec<ColocationMember>,
    #[serde(default)]
    pub role: ColocationRole,
    /// Follower: the sibling PV whose home decided the target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub followed: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZFSEvacuationStatus {
    #[serde(default)]
    pub phase: Phase,
    #[serde(default)]
    pub conditions: Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition>,
    #[serde(default)]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub pvc_ref: Option<PvcRef>,
    #[serde(default)]
    pub source: Option<SourceInfo>,
    #[serde(default)]
    pub target: Option<TargetInfo>,
    #[serde(default)]
    pub original_reclaim_policy: Option<String>,
    /// Serialized JSON of the old PV, captured post-Retain (crash recovery).
    #[serde(default)]
    pub old_pv_manifest: Option<String>,
    /// Serialized JSON of the fully-rendered replacement PV (crash recovery).
    #[serde(default)]
    pub new_pv_manifest: Option<String>,
    /// Set once the old PV delete has been issued; from here, roll-forward only.
    #[serde(default)]
    pub committed: bool,
    #[serde(default)]
    pub transfer: Option<TransferStatus>,
    /// RFC3339 time the last referencing pod disappeared (settle-period anchor).
    #[serde(default)]
    pub quiesced_at: Option<String>,
    /// RFC3339 time the PVC key was added to the VAP params (propagation grace).
    #[serde(default)]
    pub locked_at: Option<String>,
    #[serde(default)]
    pub colocation: Option<ColocationStatus>,
}

/// Param object for the ValidatingAdmissionPolicy. A single cluster-scoped
/// instance named [`PARAMS_NAME`] holds every PVC currently locked, in the
/// list matching its evacuation's mode.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[kube(
    group = "zfsevac.alumino.us",
    version = "v1alpha1",
    kind = "EvacuationParams",
    plural = "evacuationparams"
)]
#[serde(rename_all = "camelCase")]
pub struct EvacuationParamsSpec {
    /// "namespace/name" keys of PVCs that must not be referenced by new pods
    /// (WhenIdle).
    #[serde(default)]
    pub pvc_keys: Vec<String>,
    /// "namespace/name" keys of PVCs that new pods may reference as long as
    /// they go through the scheduler (WhenClaimed): pods with spec.nodeName
    /// or a toleration for the cordon taint are denied.
    #[serde(default)]
    pub claimable_pvc_keys: Vec<String>,
}

impl EvacuationParamsSpec {
    /// Put `key` in the list for `mode` and nowhere else. Returns whether
    /// anything changed.
    pub fn lock(&mut self, key: &str, mode: &EvacuationMode) -> bool {
        let (into, out_of) = match mode {
            EvacuationMode::WhenIdle => (&mut self.pvc_keys, &mut self.claimable_pvc_keys),
            EvacuationMode::WhenClaimed => (&mut self.claimable_pvc_keys, &mut self.pvc_keys),
        };
        let mut changed = false;
        if !into.iter().any(|k| k == key) {
            into.push(key.to_string());
            changed = true;
        }
        let before = out_of.len();
        out_of.retain(|k| k != key);
        changed || out_of.len() != before
    }

    /// Remove `key` from both lists. Returns whether anything changed.
    pub fn unlock(&mut self, key: &str) -> bool {
        let before = self.pvc_keys.len() + self.claimable_pvc_keys.len();
        self.pvc_keys.retain(|k| k != key);
        self.claimable_pvc_keys.retain(|k| k != key);
        self.pvc_keys.len() + self.claimable_pvc_keys.len() != before
    }
}
