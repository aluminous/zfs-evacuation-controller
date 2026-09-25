use k8s_openapi::api::core::v1::PersistentVolume;
use kube::ResourceExt;
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
fn pool_component_matching() {
    use crate::controller::target::pool_component;
    // ZFSNode inventories bare zpools; a dataset-path poolname must be
    // eligible via its zpool component (the exact-string compare used to
    // wedge TargetSelecting forever, including with spec.targetNode set).
    assert_eq!(pool_component("zroot"), "zroot");
    assert_eq!(pool_component("zroot/csi"), "zroot");
    assert_eq!(pool_component("zroot/csi/deep"), "zroot");
    assert_eq!(pool_component(""), "");
}

#[test]
fn destination_poolname_resolution() {
    use crate::controller::target::resolve_dest_poolname;
    // 1. Explicit targetPool wins verbatim.
    let (d, r) = resolve_dest_poolname(Some("tank/override"), Some("zroot/csi"), "zroot/csi");
    assert_eq!((d.as_str(), r), ("tank/override", "spec.targetPool"));
    // 2. StorageClass poolname: what provisioning on the target would use.
    let (d, r) = resolve_dest_poolname(None, Some("zroot/csi"), "zroot/old");
    assert_eq!((d.as_str(), r), ("zroot/csi", "StorageClass poolname"));
    // 3. SC gone: carry the source poolName over unchanged.
    let (d, r) = resolve_dest_poolname(None, None, "zroot/legacy");
    assert_eq!(d.as_str(), "zroot/legacy");
    assert!(r.contains("source poolName"));
}

#[test]
fn target_snapshot_cr_addresses_received_snapshot() {
    use crate::controller::transfer::target_snapshot_cr;
    use crate::crd::openebs::{VolumeInfo, ZFS_VOL_LABEL};
    use kube::ResourceExt;
    // The agent on the target builds `<poolName>/<ZFS_VOL_LABEL>@<name>` and
    // only acts on CRs whose ownerNodeID is its own — every one of those
    // must point at the received dataset, not the source.
    let target = TargetInfo {
        node: "node2".into(),
        node_id: "node2-id".into(),
        pool: "zroot/csi".into(),
        new_volume_handle: "pvc-1-abcdef0".into(),
    };
    let src_info: VolumeInfo = from_value(json!({
        "ownerNodeID": "node1-id",
        "poolName": "zroot",
        "capacity": "10737418240",
        "volumeType": "DATASET",
        "fsType": "zfs",
        "snapname": "clone-origin",
        "recordsize": "128k"
    }))
    .unwrap();
    let snap = target_snapshot_cr(&target, "zevac-a1-beef", src_info);
    assert_eq!(snap.name_any(), "zevac-a1-beef");
    assert_eq!(snap.labels()[ZFS_VOL_LABEL], "pvc-1-abcdef0");
    assert_eq!(snap.labels()["kubernetes.io/nodename"], "node2-id");
    assert_eq!(snap.spec.0.owner_node_id, "node2-id");
    assert_eq!(snap.spec.0.pool_name, "zroot/csi");
    assert_eq!(snap.spec.0.snapname, None);
    // The CRD rejects a create without status.
    assert_eq!(snap.status.as_ref().and_then(|s| s.state.as_deref()), Some("Pending"));
    // CRD-required fields and unmodeled properties ride along.
    assert_eq!(snap.spec.0.capacity.as_deref(), Some("10737418240"));
    assert_eq!(snap.spec.0.volume_type.as_deref(), Some("DATASET"));
    assert_eq!(snap.spec.0.extra["recordsize"], "128k");
}

#[test]
fn destination_handles_stay_valid_across_repeated_evacuations() {
    use crate::controller::state_machine::{
        new_destination_handle, source_handle_fits_snapshot_label,
    };
    use crate::controller::target_zfsvolume;
    use crate::controller::transfer::target_snapshot_cr;
    use crate::crd::openebs::{VolumeInfo, ZFS_VOL_LABEL};
    use std::collections::HashSet;

    // A 40-byte provisioned handle reached 64 bytes after three old-style
    // eight-byte suffixes. Each new destination becomes the next source.
    let mut source = format!("pvc-{}", "a".repeat(36));
    assert_eq!(format!("{source}-e000001-e000002-e000003").len(), 64);
    let mut seen = HashSet::from([source.clone()]);
    for _ in 0..128 {
        assert!(source_handle_fits_snapshot_label(&source));
        let handle = new_destination_handle();
        assert!(handle.len() <= 63, "{handle}");
        assert!(
            handle
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        );
        assert!(handle.starts_with('z') && handle.ends_with(|c: char| c.is_ascii_alphanumeric()));
        assert!(seen.insert(handle.clone()), "duplicate destination handle");

        let target = TargetInfo {
            node: "node-b".into(),
            node_id: "node-b-id".into(),
            pool: "tank/csi".into(),
            new_volume_handle: handle.clone(),
        };
        let recovered: TargetInfo = from_value(serde_json::to_value(&target).unwrap()).unwrap();
        assert_eq!(recovered.new_volume_handle, handle);
        let zv = target_zfsvolume(VolumeInfo::default(), &target);
        assert_eq!(zv.name_any(), handle);
        let snap = target_snapshot_cr(&target, "zevac-a1-beef", VolumeInfo::default());
        assert_eq!(snap.labels()[ZFS_VOL_LABEL], handle);
        assert_eq!(
            format!(
                "{}/{}@{}",
                target.pool,
                snap.labels()[ZFS_VOL_LABEL],
                snap.name_any()
            ),
            format!("tank/csi/{handle}@zevac-a1-beef")
        );
        source = handle;
    }
    assert_eq!(source.len(), 38);
}

#[test]
fn source_handle_must_fit_snapshot_label_without_truncation() {
    use crate::controller::state_machine::source_handle_fits_snapshot_label;

    assert!(source_handle_fits_snapshot_label(&format!(
        "pvc-{}",
        "a".repeat(59)
    )));
    assert!(!source_handle_fits_snapshot_label(&format!(
        "pvc-{}",
        "a".repeat(60)
    )));
    assert!(!source_handle_fits_snapshot_label("bad/handle"));
}

mod transfer_verdict {
    use crate::controller::transfer::{transfer_verdict, RelayObs, TransferObs, TransferVerdict};

    fn obs() -> TransferObs {
        TransferObs {
            attempt: 1,
            failure_reason: None,
            bkp_status: String::new(),
            rst_status: String::new(),
            rst_exists: false,
            mem: Some(RelayObs {
                attempt: 1,
                bytes: 0,
                last_activity: 1000,
                task_finished: false,
                source_connected: false,
            }),
            elapsed_secs: 10,
            now_secs: 1000,
            timeout_secs: 3600,
            stall_secs: 120,
        }
    }

    fn fail_reason(v: TransferVerdict) -> String {
        match v {
            TransferVerdict::FailAttempt(r) => r,
            other => panic!("expected FailAttempt, got {other:?}"),
        }
    }

    // Finding #9: Done/Done outranks a recorded failure_reason — a teardown
    // that lost the race to the agents must salvage, not destroy.
    #[test]
    fn done_wins_over_failure_reason() {
        let mut o = obs();
        o.failure_reason = Some("controller restarted mid-transfer".into());
        o.bkp_status = "Done".into();
        o.rst_status = "Done".into();
        o.mem = None;
        assert_eq!(transfer_verdict(&o), TransferVerdict::Complete { bytes: None });
    }

    // Finding #4: an attempt stuck waiting for the source must hit the
    // attempt timeout, not wait forever.
    #[test]
    fn waiting_for_source_times_out() {
        let mut o = obs();
        assert_eq!(transfer_verdict(&o), TransferVerdict::WaitingForSource);
        o.elapsed_secs = o.timeout_secs + 1;
        assert_eq!(fail_reason(transfer_verdict(&o)), "transfer timed out");
    }

    // Finding #5: a source that EOF'd into the buffer while the target is
    // still dialing must NOT trip the stall detector; the timeout bounds it.
    #[test]
    fn slow_target_is_timeout_not_stall() {
        let mut o = obs();
        o.rst_exists = true;
        let m = o.mem.as_mut().unwrap();
        m.source_connected = true;
        m.bytes = 0; // nothing delivered to the target yet
        m.last_activity = 100; // frozen long ago (source EOF)
        o.now_secs = 100 + 500; // way past stall_secs
        assert!(matches!(transfer_verdict(&o), TransferVerdict::Continue { .. }));
        o.elapsed_secs = o.timeout_secs + 1;
        assert_eq!(fail_reason(transfer_verdict(&o)), "transfer timed out");
    }

    // A flowing stream that stops moving is a stall; a flowing stream never
    // times out.
    #[test]
    fn flowing_stream_stalls_but_never_times_out() {
        let mut o = obs();
        o.rst_exists = true;
        let m = o.mem.as_mut().unwrap();
        m.source_connected = true;
        m.bytes = 1 << 20;
        m.last_activity = 990;
        o.elapsed_secs = o.timeout_secs + 500; // long transfer, still moving
        assert!(matches!(transfer_verdict(&o), TransferVerdict::Continue { .. }));
        o.now_secs = 990 + 121;
        assert_eq!(fail_reason(transfer_verdict(&o)), "transfer stalled (no bytes)");
    }

    #[test]
    fn restart_and_staleness() {
        let mut o = obs();
        o.mem = None;
        assert_eq!(fail_reason(transfer_verdict(&o)), "controller restarted mid-transfer");
        let mut o = obs();
        o.mem.as_mut().unwrap().attempt = 2;
        assert_eq!(transfer_verdict(&o), TransferVerdict::StaleStatus);
        let mut o = obs();
        o.attempt = 3;
        assert_eq!(transfer_verdict(&o), TransferVerdict::StaleRelay);
    }

    #[test]
    fn agent_failure_and_restore_creation() {
        let mut o = obs();
        o.rst_status = "Failed".into();
        o.rst_exists = true;
        assert_eq!(fail_reason(transfer_verdict(&o)), "restore reported status Failed");
        let mut o = obs();
        o.mem.as_mut().unwrap().source_connected = true;
        assert_eq!(transfer_verdict(&o), TransferVerdict::CreateRestore);
    }
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

#[test]
fn pod_scheduling_predicate() {
    use crate::controller::pod_is_scheduled;
    use k8s_openapi::api::core::v1::Pod;

    let scheduled: Pod = from_value(json!({
        "metadata": {"name": "app-0"},
        "spec": {"nodeName": "worker-1"}
    }))
    .unwrap();
    assert!(pod_is_scheduled(&scheduled));

    let pending: Pod = from_value(json!({"metadata": {"name": "app-1"}, "spec": {}})).unwrap();
    assert!(!pod_is_scheduled(&pending));

    let empty: Pod =
        from_value(json!({"metadata": {"name": "app-2"}, "spec": {"nodeName": ""}})).unwrap();
    assert!(!pod_is_scheduled(&empty));
}

#[test]
fn blocking_pods_message_separates_remedies() {
    use crate::controller::blocking_pods_message;
    use k8s_openapi::api::core::v1::Pod;

    let running: Pod = from_value(json!({
        "metadata": {"name": "running-consumer"},
        "spec": {"nodeName": "worker-1"}
    }))
    .unwrap();
    let stranded: Pod =
        from_value(json!({"metadata": {"name": "stranded-replacement"}, "spec": {}})).unwrap();

    let msg = blocking_pods_message(std::slice::from_ref(&running));
    assert_eq!(msg, "waiting for pods to release PVC: running-consumer");

    // A never-scheduled pod predates the lock; the only remedy is deletion,
    // and the message must say so rather than imply it will drain away.
    let msg = blocking_pods_message(std::slice::from_ref(&stranded));
    assert!(
        msg.starts_with("never-scheduled pods predate the lock"),
        "{msg}"
    );
    assert!(
        msg.contains("stranded-replacement") && msg.contains("delete them"),
        "{msg}"
    );

    let msg = blocking_pods_message(&[running, stranded]);
    assert!(
        msg.contains("running-consumer") && msg.contains("stranded-replacement"),
        "{msg}"
    );
}

/// The relay must take the source's bytes before the target exists (the
/// agents' `nc -w 3` dies on a 3 s idle socket) and hand them over intact
/// once it does.
#[tokio::test]
async fn relay_buffers_source_until_target_connects() {
    use crate::transfer::relay::{IpNet, Relay};
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let lo = vec![IpNet::host("127.0.0.1".parse().unwrap())];
    let relay = Relay::spawn(lo.clone(), lo).await.unwrap();
    assert!(!relay.source_connected.load(Ordering::Acquire));

    let payload: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    let mut src = TcpStream::connect(("127.0.0.1", relay.backup_port)).await.unwrap();
    src.write_all(&payload).await.unwrap();
    src.shutdown().await.unwrap();
    // Everything was accepted with no target in sight: the source is done.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(relay.source_connected.load(Ordering::Acquire));
    assert_eq!(relay.bytes.load(Ordering::Relaxed), 0, "nothing delivered yet");
    assert!(!relay.task.is_finished());

    let mut dst = TcpStream::connect(("127.0.0.1", relay.restore_port)).await.unwrap();
    let mut got = Vec::new();
    dst.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, payload);
    assert_eq!(relay.task.await.unwrap().unwrap(), payload.len() as u64);
}

#[test]
fn params_lock_lists_follow_mode() {
    use crate::crd::zfs_evacuation::{EvacuationMode, EvacuationParamsSpec};

    let mut p = EvacuationParamsSpec::default();
    assert!(p.lock("ns/a", &EvacuationMode::WhenIdle));
    assert!(!p.lock("ns/a", &EvacuationMode::WhenIdle), "idempotent");
    assert_eq!(p.pvc_keys, vec!["ns/a"]);
    assert!(p.claimable_pvc_keys.is_empty());

    // Re-locking under the other mode moves the key, never duplicates it.
    assert!(p.lock("ns/a", &EvacuationMode::WhenClaimed));
    assert!(p.pvc_keys.is_empty());
    assert_eq!(p.claimable_pvc_keys, vec!["ns/a"]);

    assert!(p.lock("ns/b", &EvacuationMode::WhenIdle));
    assert!(p.unlock("ns/a"));
    assert!(!p.unlock("ns/a"));
    assert_eq!(p.pvc_keys, vec!["ns/b"]);
    assert!(p.claimable_pvc_keys.is_empty());
}

#[test]
fn node_repels_pods_cases() {
    use crate::controller::node_repels_pods;
    use k8s_openapi::api::core::v1::Node;
    let key = "zfsevac.alumino.us/evacuate";

    let node = |spec: serde_json::Value| -> Node {
        from_value(json!({"metadata": {"name": "n"}, "spec": spec})).unwrap()
    };
    assert!(!node_repels_pods(&node(json!({})), key));
    assert!(node_repels_pods(&node(json!({"unschedulable": true})), key));
    // The soft effect the trigger uses contributes nothing; the cordon does.
    assert!(!node_repels_pods(
        &node(json!({"taints": [{"key": key, "effect": "PreferNoSchedule"}]})),
        key
    ));
    assert!(node_repels_pods(
        &node(json!({"taints": [{"key": key, "effect": "NoSchedule"}]})),
        key
    ));
    // Someone else's hard taint may be tolerated by the consumer; not ours to judge.
    assert!(!node_repels_pods(
        &node(json!({"taints": [{"key": "other", "effect": "NoSchedule"}]})),
        key
    ));
}

#[test]
fn placement_honours_pod_constraints() {
    use crate::controller::placement::Placement;
    use k8s_openapi::api::core::v1::{Node, Pod};

    let node = |name: &str, labels: serde_json::Value, taints: serde_json::Value| -> Node {
        from_value(json!({
            "metadata": {"name": name, "labels": labels},
            "spec": {"taints": taints}
        }))
        .unwrap()
    };
    let gpu = node("gpu-1", json!({"tier": "gpu", "zone": "a", "cores": "32"}), json!([]));
    let plain = node("plain-1", json!({"tier": "plain", "zone": "b", "cores": "8"}), json!([]));
    let tainted = node(
        "dedicated-1",
        json!({"tier": "gpu"}),
        json!([{"key": "dedicated", "value": "ml", "effect": "NoSchedule"}]),
    );
    let soft = node(
        "soft-1",
        json!({"tier": "gpu"}),
        json!([{"key": "dedicated", "effect": "PreferNoSchedule"}]),
    );

    let pod = |spec: serde_json::Value| -> Pod {
        from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
    };

    // No constraints: everything without a hard taint is fine.
    let any = Placement::from_pod(&pod(json!({})));
    assert!(any.admits(&gpu) && any.admits(&plain) && any.admits(&soft));
    assert!(!any.admits(&tainted));

    let sel = Placement::from_pod(&pod(json!({"nodeSelector": {"tier": "gpu"}})));
    assert!(sel.admits(&gpu) && !sel.admits(&plain));

    let affinity = Placement::from_pod(&pod(json!({"affinity": {"nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [
                {"key": "zone", "operator": "NotIn", "values": ["b"]},
                {"key": "cores", "operator": "Gt", "values": ["16"]}
            ]},
            {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["plain-1"]}]}
        ]}}}})));
    assert!(affinity.admits(&gpu), "first term");
    assert!(affinity.admits(&plain), "second term (by name)");
    assert!(!affinity.admits(&soft), "neither term: no zone/cores labels, wrong name");

    // Preferred affinity is not a constraint.
    let preferred = Placement::from_pod(&pod(json!({"affinity": {"nodeAffinity": {
        "preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 1, "preference": {
            "matchExpressions": [{"key": "zone", "operator": "In", "values": ["z"]}]}}]}}})));
    assert!(preferred.admits(&plain));

    let tolerant = Placement::from_pod(&pod(json!({"tolerations": [
        {"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}
    ]})));
    assert!(tolerant.admits(&tainted));
    let wrong_value = Placement::from_pod(&pod(json!({"tolerations": [
        {"key": "dedicated", "operator": "Equal", "value": "batch", "effect": "NoSchedule"}
    ]})));
    assert!(!wrong_value.admits(&tainted));
    let exists_all = Placement::from_pod(&pod(json!({"tolerations": [{"operator": "Exists"}]})));
    assert!(exists_all.admits(&tainted));
    let no_execute_only = Placement::from_pod(&pod(json!({"tolerations": [
        {"operator": "Exists", "effect": "NoExecute"}
    ]})));
    assert!(!no_execute_only.admits(&tainted));
}

#[test]
fn anchor_majority_then_name() {
    use crate::controller::colocation::{pick_anchor, Anchor};
    let a = |node: &str, pv: &str| Anchor { node: node.into(), pv_name: pv.into(), how: "test" };

    assert_eq!(pick_anchor(vec![]), None);
    assert_eq!(pick_anchor(vec![a("n2", "pv-1")]).unwrap().node, "n2");
    // Majority wins: fewest volumes have to move again.
    let split = pick_anchor(vec![a("n2", "pv-1"), a("n3", "pv-2"), a("n2", "pv-3")]).unwrap();
    assert_eq!((split.node.as_str(), split.pv_name.as_str()), ("n2", "pv-1"));
    // Ties are broken by node string so every reconcile agrees.
    assert_eq!(pick_anchor(vec![a("n3", "x"), a("n2", "y")]).unwrap().node, "n2");
}

#[test]
fn pod_pvc_names_and_consumer_order() {
    use crate::controller::{pod_pvc_names, unscheduled_pods};
    use k8s_openapi::api::core::v1::Pod;

    let pod: Pod = from_value(json!({
        "metadata": {"name": "worker"},
        "spec": {"volumes": [
            {"name": "data", "persistentVolumeClaim": {"claimName": "data-0"}},
            {"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}},
            {"name": "cfg", "configMap": {"name": "cfg"}}
        ]}
    }))
    .unwrap();
    assert_eq!(pod_pvc_names(&pod), vec!["data-0", "worker-scratch"]);

    let mk = |name: &str, node: Option<&str>| -> Pod {
        let mut spec = json!({});
        if let Some(n) = node {
            spec["nodeName"] = json!(n);
        }
        from_value(json!({"metadata": {"name": name}, "spec": spec})).unwrap()
    };
    let pods = vec![mk("z-pending", None), mk("a-running", Some("n1")), mk("b-pending", None)];
    let names: Vec<String> = unscheduled_pods(&pods).iter().map(|p| p.name_any()).collect();
    assert_eq!(names, vec!["b-pending", "z-pending"]);
}
