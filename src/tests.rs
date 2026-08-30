use k8s_openapi::api::core::v1::PersistentVolume;
use serde_json::{from_value, json};

use crate::controller::pv_swap::render_new_pv;
use crate::controller::{parse_quantity_or_bytes, pod_references_pvc};
use crate::crd::zfs_evacuation::{PvcRef, TargetInfo};

#[test]
fn quantity_parsing() {
    assert_eq!(parse_quantity_or_bytes("1073741824"), Some(1 << 30));
    assert_eq!(parse_quantity_or_bytes("1Gi"), Some(1 << 30));
    assert_eq!(parse_quantity_or_bytes("500Mi"), Some(500 << 20));
    assert_eq!(parse_quantity_or_bytes("2G"), Some(2_000_000_000));
    assert_eq!(parse_quantity_or_bytes("1.5Gi"), Some(3 << 29));
    assert_eq!(parse_quantity_or_bytes("garbage"), None);
    assert_eq!(parse_quantity_or_bytes("-5Gi"), None);
}

#[test]
fn pod_pvc_references() {
    let direct = from_value(json!({
        "metadata": {"name": "app-0"},
        "spec": {"volumes": [
            {"name": "data", "persistentVolumeClaim": {"claimName": "data-app-0"}}
        ]}
    }))
    .unwrap();
    assert!(pod_references_pvc(&direct, "data-app-0"));
    assert!(!pod_references_pvc(&direct, "other"));

    // Generic ephemeral volume: PVC name is derived as <pod>-<volume>.
    let ephemeral = from_value(json!({
        "metadata": {"name": "worker"},
        "spec": {"volumes": [
            {"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}
        ]}
    }))
    .unwrap();
    assert!(pod_references_pvc(&ephemeral, "worker-scratch"));
    assert!(!pod_references_pvc(&ephemeral, "scratch"));

    let none = from_value(json!({"metadata": {"name": "plain"}, "spec": {}})).unwrap();
    assert!(!pod_references_pvc(&none, "data-app-0"));
}

fn old_pv() -> PersistentVolume {
    from_value(json!({
        "metadata": {
            "name": "pvc-1234",
            "uid": "old-uid",
            "resourceVersion": "42",
            "annotations": {"pv.kubernetes.io/provisioned-by": "zfs.csi.openebs.io"},
            "finalizers": ["kubernetes.io/pv-protection"]
        },
        "spec": {
            "capacity": {"storage": "8Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Delete",
            "storageClassName": "zfs",
            "claimRef": {"namespace": "default", "name": "data", "uid": "pvc-uid-1"},
            "csi": {
                "driver": "zfs.csi.openebs.io",
                "volumeHandle": "pvc-1234",
                "fsType": "zfs",
                "volumeAttributes": {"openebs.io/poolname": "tank", "openebs.io/cas-type": "localpv-zfs"}
            },
            "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                {"key": "openebs.io/nodeid", "operator": "In", "values": ["node-a"]}
            ]}]}}
        },
        "status": {"phase": "Bound"}
    }))
    .unwrap()
}

#[test]
fn new_pv_rendering() {
    let pvc_ref = PvcRef {
        namespace: "default".into(),
        name: "data".into(),
        uid: "pvc-uid-1".into(),
    };
    let target = TargetInfo {
        node: "node-b".into(),
        node_id: "node-b-id".into(),
        pool: "tank2".into(),
        new_volume_handle: "pvc-1234-e00abc".into(),
    };
    let new_pv = render_new_pv(&old_pv(), &pvc_ref, &target).unwrap();

    // Same PV name — this is what keeps the PVC binding intact.
    assert_eq!(new_pv.metadata.name.as_deref(), Some("pvc-1234"));
    // Server-populated metadata must not be carried over.
    assert!(new_pv.metadata.uid.is_none());
    assert!(new_pv.metadata.resource_version.is_none());
    assert!(new_pv.metadata.finalizers.is_none());
    assert!(new_pv.status.is_none());
    // provisioned-by survives so the provisioner reclaims the PV at end-of-life.
    assert_eq!(
        new_pv.metadata.annotations.as_ref().unwrap()["pv.kubernetes.io/provisioned-by"],
        "zfs.csi.openebs.io"
    );

    let spec = new_pv.spec.as_ref().unwrap();
    // Pre-bound claimRef with the PVC's UID drives automatic rebinding.
    let claim = spec.claim_ref.as_ref().unwrap();
    assert_eq!(claim.uid.as_deref(), Some("pvc-uid-1"));
    // Comes up as Retain regardless of the original policy.
    assert_eq!(spec.persistent_volume_reclaim_policy.as_deref(), Some("Retain"));
    // Rewritten CSI source.
    let csi = spec.csi.as_ref().unwrap();
    assert_eq!(csi.volume_handle, "pvc-1234-e00abc");
    assert_eq!(csi.volume_attributes.as_ref().unwrap()["openebs.io/poolname"], "tank2");
    // Node affinity points at the target's node id.
    let affinity = serde_json::to_value(spec.node_affinity.as_ref().unwrap()).unwrap();
    assert_eq!(
        affinity["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0],
        "node-b-id"
    );
}

#[test]
fn ipnet_matching() {
    use crate::transfer::relay::IpNet;
    let node = IpNet::host("100.71.177.44".parse().unwrap());
    assert!(node.contains("100.71.177.44".parse().unwrap()));
    assert!(!node.contains("100.71.177.45".parse().unwrap()));

    let pod_cidr = IpNet::parse_cidr("10.42.1.0/24").unwrap();
    // The flannel gateway address that bit us in production testing.
    assert!(pod_cidr.contains("10.42.1.1".parse().unwrap()));
    assert!(pod_cidr.contains("10.42.1.254".parse().unwrap()));
    assert!(!pod_cidr.contains("10.42.2.1".parse().unwrap()));

    assert!(IpNet::parse_cidr("10.42.1.0/33").is_none());
    assert!(IpNet::parse_cidr("garbage").is_none());
}

#[test]
fn taint_matching() {
    use crate::controller::node_has_evacuate_taint;
    let key = "zfsevac.alumino.us/evacuate";
    let tainted = from_value(json!({
        "metadata": {"name": "n1"},
        "spec": {"taints": [{"key": key, "effect": "PreferNoSchedule"}]}
    }))
    .unwrap();
    assert!(node_has_evacuate_taint(&tainted, key));
    // Any effect counts; other keys don't.
    let other = from_value(json!({
        "metadata": {"name": "n2"},
        "spec": {"taints": [{"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}]}
    }))
    .unwrap();
    assert!(!node_has_evacuate_taint(&other, key));
    let bare = from_value(json!({"metadata": {"name": "n3"}, "spec": {}})).unwrap();
    assert!(!node_has_evacuate_taint(&bare, key));
}

#[test]
fn crd_generation_is_valid() {
    use kube::CustomResourceExt;
    let crd = crate::crd::zfs_evacuation::ZFSEvacuation::crd();
    assert_eq!(crd.spec.names.kind, "ZFSEvacuation");
    assert_eq!(crd.spec.scope, "Cluster");
    let crd = crate::crd::zfs_evacuation::EvacuationParams::crd();
    assert_eq!(crd.spec.names.kind, "EvacuationParams");
}
