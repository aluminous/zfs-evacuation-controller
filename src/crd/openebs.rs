//! Typed definitions for the openebs zfs-localpv CRs we consume.
//!
//! These CRDs are owned and installed by zfs-localpv; we only need enough of
//! their shape to read and write them. Unknown/extra spec fields are preserved
//! through the `extra` flatten maps so that copying a spec (e.g. into a
//! ZFSRestore volSpec) round-trips properties we don't model (recordsize,
//! compression, encryption, ...).

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::NamespaceResourceScope;
use kube::CustomResource;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const OPENEBS_GROUP: &str = "zfs.openebs.io";
/// Finalizer the zfs-localpv node agent places on ZFSVolume/ZFSSnapshot CRs
/// and removes only after a successful `zfs destroy`.
pub const ZFS_FINALIZER: &str = "zfs.openebs.io/finalizer";
/// Label linking ZFSSnapshot CRs to their source volume.
pub const ZFS_VOL_LABEL: &str = "openebs.io/persistent-volume";
/// Annotation the CSI controller uses to defer volume deletion while
/// snapshots exist.
pub const MARKED_FOR_DELETION: &str = "openebs.io/marked-for-deletion";
/// Topology key used in PV nodeAffinity, and node label overriding node identity.
pub const NODE_ID_TOPOLOGY_KEY: &str = "openebs.io/nodeid";
/// volumeAttributes key on the PV naming the pool.
pub const POOLNAME_ATTRIBUTE: &str = "openebs.io/poolname";
/// The CSI driver name.
pub const ZFS_DRIVER: &str = "zfs.csi.openebs.io";

/// zfs-localpv `VolumeInfo` — the spec of ZFSVolume/ZFSSnapshot and the
/// `volSpec` of ZFSRestore. Only fields we act on are modeled; the rest ride
/// in `extra` verbatim.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct VolumeInfo {
    #[serde(rename = "ownerNodeID")]
    pub owner_node_id: String,
    #[serde(rename = "poolName")]
    pub pool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    /// Set only on clones (name of the origin snapshot).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapname: Option<String>,
    #[serde(rename = "fsType", default, skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(rename = "volumeType", default, skip_serializing_if = "Option::is_none")]
    pub volume_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ZFSVolumeStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

pub const ZFS_STATUS_READY: &str = "Ready";
pub const ZFS_STATUS_PENDING: &str = "Pending";

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug)]
#[kube(
    group = "zfs.openebs.io",
    version = "v1",
    kind = "ZFSVolume",
    plural = "zfsvolumes",
    namespaced,
    schema = "disabled",
    status = "ZFSVolumeStatus"
)]
#[serde(transparent)]
pub struct ZFSVolumeSpec(pub VolumeInfo);

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug)]
#[kube(
    group = "zfs.openebs.io",
    version = "v1",
    kind = "ZFSSnapshot",
    plural = "zfssnapshots",
    namespaced,
    schema = "disabled"
)]
#[serde(transparent)]
pub struct ZFSSnapshotSpec(pub VolumeInfo);

/// ZFSBackup/ZFSRestore statuses are plain strings on these CRDs
/// (Init/Pending/InProgress/Done/Failed/Invalid), set to Init by the creator.
pub const BKP_STATUS_INIT: &str = "Init";
pub const BKP_STATUS_DONE: &str = "Done";
pub const BKP_STATUS_FAILED: &str = "Failed";
pub const BKP_STATUS_INVALID: &str = "Invalid";

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default)]
#[kube(
    group = "zfs.openebs.io",
    version = "v1",
    kind = "ZFSBackup",
    plural = "zfsbackups",
    namespaced,
    schema = "disabled",
    status = "String"
)]
pub struct ZFSBackupSpec {
    #[serde(rename = "volumeName")]
    pub volume_name: String,
    #[serde(rename = "ownerNodeID")]
    pub owner_node_id: String,
    #[serde(rename = "snapName", default, skip_serializing_if = "Option::is_none")]
    pub snap_name: Option<String>,
    #[serde(rename = "prevSnapName", default, skip_serializing_if = "Option::is_none")]
    pub prev_snap_name: Option<String>,
    /// "ip:port" the source node agent connects out to with the send stream.
    #[serde(rename = "backupDest")]
    pub backup_dest: String,
}

/// ZFSRestore's `volSpec` is a TOP-LEVEL field (sibling of `spec`) in the
/// real CRD — the CustomResource derive can't express that, so this one is
/// hand-rolled like ZFSNode.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ZFSRestore {
    // Hand-rolled types must carry TypeMeta themselves: create bodies without
    // apiVersion/kind are rejected ("Object 'Kind' is missing"). Defaulted on
    // deserialize because list items omit it.
    #[serde(flatten, default)]
    pub type_meta: kube::core::TypeMeta,
    pub metadata: ObjectMeta,
    pub spec: ZFSRestoreSpec,
    #[serde(rename = "volSpec", default, skip_serializing_if = "Option::is_none")]
    pub vol_spec: Option<VolumeInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ZFSRestoreSpec {
    /// Name of the dataset to `zfs recv` into (== new ZFSVolume CR name).
    #[serde(rename = "volumeName")]
    pub volume_name: String,
    #[serde(rename = "ownerNodeID")]
    pub owner_node_id: String,
    /// "ip:port" the target node agent connects out to for the stream.
    #[serde(rename = "restoreSrc")]
    pub restore_src: String,
}

impl ZFSRestore {
    pub fn new(name: &str, spec: ZFSRestoreSpec, vol_spec: VolumeInfo) -> Self {
        ZFSRestore {
            type_meta: kube::core::TypeMeta {
                api_version: format!("{OPENEBS_GROUP}/v1"),
                kind: "ZFSRestore".into(),
            },
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec,
            vol_spec: Some(vol_spec),
            status: None,
        }
    }
}

impl kube::Resource for ZFSRestore {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind(_: &()) -> std::borrow::Cow<'static, str> {
        "ZFSRestore".into()
    }
    fn group(_: &()) -> std::borrow::Cow<'static, str> {
        OPENEBS_GROUP.into()
    }
    fn version(_: &()) -> std::borrow::Cow<'static, str> {
        "v1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'static, str> {
        "zfsrestores".into()
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// ZFSNode has no spec/status: `pools` sits at the top level, so the
/// CustomResource derive (which imposes a spec) doesn't fit — implement
/// kube::Resource by hand.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ZFSNode {
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub pools: Vec<ZfsPool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ZfsPool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    /// ZFS `available` for the pool root dataset (resource.Quantity string).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free: Option<k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
}

impl kube::Resource for ZFSNode {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind(_: &()) -> std::borrow::Cow<'static, str> {
        "ZFSNode".into()
    }
    fn group(_: &()) -> std::borrow::Cow<'static, str> {
        OPENEBS_GROUP.into()
    }
    fn version(_: &()) -> std::borrow::Cow<'static, str> {
        "v1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'static, str> {
        "zfsnodes".into()
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
