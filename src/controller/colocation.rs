//! WhenClaimed grouping: the never-scheduled consumer of a locked PVC
//! defines the set of volumes that must land on one node, and where that
//! node is if any of them already has a home. No labels, no bookkeeping —
//! the pod that will mount the volumes is the only source of truth that
//! exists exactly when it matters.

use std::collections::HashMap;

use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use kube::ResourceExt;

use crate::controller::placement::Placement;
use crate::controller::{parse_quantity_or_bytes, pod_pvc_names, unscheduled_pods, Ctx};
use crate::crd::openebs::{ZFSVolume, ZFS_DRIVER};
use crate::crd::zfs_evacuation::{ColocationMember, Phase, PvcRef, ZFSEvacuation};

/// A sibling that already has (or is committed to) a node off the source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Anchor {
    /// Node name or node id — `select_target` accepts either.
    pub node: String,
    pub pv_name: String,
    pub how: &'static str,
}

#[derive(Clone, Debug)]
pub struct Group {
    /// "namespace/name" of the consumer.
    pub pod: String,
    pub placement: Placement,
    /// Volumes still on the source that no sibling evacuation has placed
    /// yet — this one included. Their capacity travels with the leader.
    pub members: Vec<ColocationMember>,
    pub anchor: Option<Anchor>,
}

/// Resolve the group for `pv_name` from the pods referencing its PVC.
/// None when no never-scheduled consumer exists (nothing to co-locate with).
pub async fn resolve(
    ctx: &Ctx,
    pv_name: &str,
    pvc_ref: &PvcRef,
    source_node_id: &str,
    pods: &[Pod],
) -> Result<Option<Group>> {
    let Some(pod) = unscheduled_pods(pods).into_iter().next() else {
        return Ok(None);
    };
    let evacs: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let zv_api: Api<ZFSVolume> = ctx.openebs();
    let mut members = Vec::new();
    let mut anchors: Vec<Anchor> = Vec::new();

    for pvc_name in pod_pvc_names(pod) {
        let Some(pvc) = ctx.pvcs(&pvc_ref.namespace).get_opt(&pvc_name).await? else {
            continue;
        };
        let Some(member_pv) = pvc.spec.as_ref().and_then(|s| s.volume_name.clone()) else {
            continue;
        };
        let Some(pv) = ctx.pvs().get_opt(&member_pv).await? else {
            continue;
        };
        let Some(csi) = pv.spec.as_ref().and_then(|s| s.csi.clone()) else {
            continue;
        };
        if csi.driver != ZFS_DRIVER {
            continue;
        }
        let Some(zv) = zv_api.get_opt(&csi.volume_handle).await? else {
            continue;
        };
        let capacity_bytes = zv
            .spec
            .0
            .capacity
            .as_deref()
            .and_then(parse_quantity_or_bytes)
            .unwrap_or(0) as u64;
        let member = ColocationMember { pv_name: member_pv.clone(), capacity_bytes };
        if member_pv == pv_name {
            members.push(member);
            continue;
        }
        // A sibling's evacuation, if it has one, knows more than its
        // ZFSVolume: an explicit or chosen target is where the data is
        // heading even while the dataset still sits on the source.
        let sibling = evacs.get_opt(&member_pv).await?.filter(|e| {
            !e.status
                .as_ref()
                .is_some_and(|s| matches!(s.phase, Phase::Completed | Phase::Failed | Phase::Aborting))
        });
        if let Some(node) = sibling.as_ref().and_then(|e| e.spec.target_node.clone()) {
            anchors.push(Anchor { node, pv_name: member_pv, how: "spec.targetNode" });
        } else if let Some(t) = sibling.as_ref().and_then(|e| e.status.as_ref()?.target.clone()) {
            anchors.push(Anchor { node: t.node_id, pv_name: member_pv, how: "status.target" });
        } else if zv.spec.0.owner_node_id != source_node_id {
            anchors.push(Anchor {
                node: zv.spec.0.owner_node_id.clone(),
                pv_name: member_pv,
                how: "ZFSVolume owner",
            });
        } else {
            members.push(member);
        }
    }

    Ok(Some(Group {
        pod: format!("{}/{}", pvc_ref.namespace, pod.name_any()),
        placement: Placement::from_pod(pod),
        members,
        anchor: pick_anchor(anchors),
    }))
}

/// Siblings normally agree; when they do not (a group already split), go
/// with the majority so the fewest volumes have to move again, ties by
/// node string for determinism across reconciles.
pub fn pick_anchor(anchors: Vec<Anchor>) -> Option<Anchor> {
    if anchors.len() > 1 {
        let distinct: std::collections::BTreeSet<&str> =
            anchors.iter().map(|a| a.node.as_str()).collect();
        if distinct.len() > 1 {
            tracing::warn!(?anchors, "co-location siblings disagree on a node; following the majority");
        }
    }
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for a in &anchors {
        *votes.entry(a.node.as_str()).or_default() += 1;
    }
    let best = votes
        .iter()
        .max_by(|(na, ca), (nb, cb)| ca.cmp(cb).then_with(|| nb.cmp(na)))
        .map(|(n, _)| n.to_string())?;
    anchors.into_iter().find(|a| a.node == best)
}
