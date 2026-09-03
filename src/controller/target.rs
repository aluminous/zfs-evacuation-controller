//! Target node/pool selection: ZFSNode capacity (≤60s stale) minus a
//! reservation ledger rebuilt from in-flight ZFSEvacuation statuses, filtered
//! by node health and StorageClass allowedTopologies.

use anyhow::{anyhow, bail, Result};
use k8s_openapi::api::core::v1::PersistentVolume;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::{Api, ListParams};
use kube::ResourceExt;

use crate::crd::openebs::{ZFSNode, NODE_ID_TOPOLOGY_KEY};
use crate::crd::zfs_evacuation::{Phase, TargetInfo, ZFSEvacuation, ZFSEvacuationSpec};
use crate::controller::placement::Placement;
use crate::controller::{
    node_id_of, node_is_ready, parse_quantity_or_bytes, Ctx, DEFAULT_HEADROOM_PERCENT,
};

pub struct SelectionInput<'a> {
    pub evac_name: &'a str,
    pub spec: &'a ZFSEvacuationSpec,
    pub source_node_id: &'a str,
    pub source_pool: &'a str,
    pub capacity_bytes: u128,
    pub pv: &'a PersistentVolume,
    /// Co-location: bytes of the group's other unplaced members, carried
    /// along so the leader's target can take the whole group.
    pub group_extra_bytes: u128,
    /// Co-location: a sibling's node (name or id) this volume must follow.
    /// `spec.targetNode` still wins when both are set.
    pub anchor: Option<&'a str>,
    /// Co-location: the consumer's own node constraints.
    pub placement: Option<&'a Placement>,
}

/// A zfs-localpv poolname may be a dataset path ("zroot/csi"). Only its
/// first component names a zpool; ZFSNode inventories (and capacity) are
/// per-zpool, so eligibility must compare components while the full path
/// stays the receive location.
pub fn pool_component(poolname: &str) -> &str {
    poolname.split('/').next().unwrap_or(poolname)
}

/// Where the received dataset goes (the new ZFSVolume's poolName), in
/// priority order — none of which encodes any site naming convention:
/// 1. spec.targetPool verbatim (operator override);
/// 2. the PV's StorageClass `poolname` parameter: by definition what
///    provisioning this PVC on the target node would have used;
/// 3. the source volume's poolName carried over (SC gone or missing the
///    parameter).
///
/// Returns the destination and which rule picked it (for logging).
pub fn resolve_dest_poolname(
    target_pool: Option<&str>,
    sc_poolname: Option<&str>,
    source_pool: &str,
) -> (String, &'static str) {
    if let Some(tp) = target_pool {
        return (tp.to_string(), "spec.targetPool");
    }
    if let Some(sc) = sc_poolname {
        return (sc.to_string(), "StorageClass poolname");
    }
    (source_pool.to_string(), "source poolName (StorageClass unavailable)")
}

pub async fn select_target(ctx: &Ctx, input: &SelectionInput<'_>) -> Result<TargetInfo> {
    let zfsnodes: Api<ZFSNode> = ctx.openebs();
    let nodes = ctx.nodes().list(&ListParams::default()).await?;
    let evacs: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let all_evacs = evacs.list(&ListParams::default()).await?;

    let sc_pool = sc_poolname(ctx, input.pv).await?;
    let (dest_pool, dest_rule) = resolve_dest_poolname(
        input.spec.target_pool.as_deref(),
        sc_pool.as_deref(),
        input.source_pool,
    );
    let wanted_component = pool_component(&dest_pool).to_string();
    tracing::info!(
        evac = input.evac_name,
        dest = %dest_pool,
        rule = dest_rule,
        "destination poolname resolved"
    );
    let headroom = input
        .spec
        .headroom_percent
        .unwrap_or(DEFAULT_HEADROOM_PERCENT) as u128;
    let bytes = input.capacity_bytes + input.group_extra_bytes;
    let need = bytes + bytes * headroom / 100;

    // Reservation ledger + set of nodes already involved in an evacuation
    // (concurrency cap: 1 inbound + 1 outbound per node).
    let placed: std::collections::HashSet<String> = all_evacs
        .iter()
        .filter(|e| e.status.as_ref().is_some_and(|s| s.target.is_some()))
        .map(|e| e.name_any())
        .collect();
    let mut reserved: std::collections::HashMap<(String, String), u128> = Default::default();
    let mut busy_targets: std::collections::HashSet<String> = Default::default();
    for e in &all_evacs {
        if e.name_any() == input.evac_name {
            continue;
        }
        let Some(st) = &e.status else { continue };
        // Failed evacuations reserve nothing (their target never kept data).
        if st.phase == Phase::Failed {
            continue;
        }
        let completed = st.phase == Phase::Completed;
        if let Some(t) = &st.target {
            // status.target.pool may be a dataset path; capacity is a
            // per-zpool quantity, so the ledger keys on the pool component.
            let slot = reserved
                .entry((t.node_id.clone(), pool_component(&t.pool).to_string()))
                .or_default();
            if !completed {
                busy_targets.insert(t.node_id.clone());
                *slot += capacity_of_evac(e);
            }
            // A co-location leader also holds its group's unplaced members —
            // and keeps holding them AFTER it completes: busy_targets blocked
            // the followers from selecting for the leader's whole active
            // life, so releasing the group reservation at Completed would let
            // an unrelated evacuation take the anchor node's space before any
            // follower had a turn, wedging the anchored group forever. Each
            // member's share moves to its own evacuation the moment that one
            // picks a target (and never counts against the member itself
            // while it is choosing).
            if let Some(c) = &st.colocation {
                *slot += c
                    .members
                    .iter()
                    .filter(|m| m.pv_name != e.name_any())
                    .filter(|m| m.pv_name != input.evac_name && !placed.contains(&m.pv_name))
                    .map(|m| m.capacity_bytes as u128)
                    .sum::<u128>();
            }
        }
    }

    // Allowed node ids from the StorageClass topology, if constrained.
    let allowed_ids = allowed_node_ids(ctx, input.pv).await?;

    let explicit = input.spec.target_node.as_deref().or(input.anchor);
    let mut candidates = Vec::new();
    let mut rejected: Vec<String> = Vec::new();
    for zn in zfsnodes.list(&ListParams::default()).await? {
        let node_id = zn.name_any();
        if node_id == input.source_node_id {
            continue;
        }
        if let Some(explicit) = explicit {
            // Explicit target may be given as node name or node id.
            let matches_explicit = nodes
                .iter()
                .find(|n| n.name_any() == *explicit)
                .map(|n| node_id_of(n) == node_id)
                .unwrap_or(false)
                || *explicit == node_id;
            if !matches_explicit {
                continue;
            }
        }
        let mut reject = |why: String| rejected.push(format!("{node_id}: {why}"));
        if let Some(allowed) = &allowed_ids
            && !allowed.contains(&node_id) {
                reject("outside the StorageClass allowedTopologies".into());
                continue;
            }
        let Some(node) = nodes.iter().find(|n| node_id_of(n) == node_id) else {
            reject("no Node with this openebs.io/nodeid".into());
            continue;
        };
        if !node_is_ready(node) {
            reject("not Ready or unschedulable".into());
            continue;
        }
        // A node marked for evacuation must never receive evacuated data.
        if crate::controller::node_has_evacuate_taint(node, &ctx.cfg.taint_key) {
            reject("carries the evacuate taint".into());
            continue;
        }
        if busy_targets.contains(&node_id) {
            reject("already receiving another evacuation".into());
            continue;
        }
        if let Some(p) = input.placement
            && !p.admits(node) {
                reject("excluded by the consumer pod's nodeSelector/affinity/tolerations".into());
                continue;
            }
        let mut has_pool = false;
        for pool in &zn.pools {
            // ZFSNode reports bare zpool names; the destination may be a
            // dataset path within one. Eligibility is a zpool property.
            if pool.name != wanted_component {
                continue;
            }
            has_pool = true;
            let free = pool
                .free
                .as_ref()
                .and_then(|q| parse_quantity_or_bytes(&q.0))
                .unwrap_or(0);
            let res = reserved
                .get(&(node_id.clone(), pool.name.clone()))
                .copied()
                .unwrap_or(0);
            if free.saturating_sub(res) >= need {
                candidates.push((node.name_any(), node_id.clone(), pool.name.clone(), free - res));
            } else {
                reject(format!(
                    "zpool {wanted_component} has {} bytes free after reservations, need {need}",
                    free.saturating_sub(res)
                ));
            }
        }
        if !has_pool {
            reject(format!("no zpool {wanted_component}"));
        }
    }

    // Most free space first.
    candidates.sort_by_key(|c| std::cmp::Reverse(c.3));
    let (node, node_id, _, _) = candidates.into_iter().next().ok_or_else(|| {
        let reasons = if rejected.is_empty() {
            String::new()
        } else {
            format!("; {}", rejected.join(", "))
        };
        match explicit {
            Some(e) => anyhow!("target {e} ineligible (need {need} bytes in zpool {wanted_component}){reasons}"),
            None => anyhow!(
                "no eligible target: need {need} bytes in zpool {wanted_component} \
                 (for destination {dest_pool}) on a ready node (excluding source {}){reasons}",
                input.source_node_id
            ),
        }
    })?;

    Ok(TargetInfo {
        node,
        node_id,
        // The full destination poolname: becomes the new ZFSVolume's
        // poolName, i.e. the parent the target agent receives into.
        pool: dest_pool,
        new_volume_handle: String::new(), // filled by caller
    })
}

/// The PV's StorageClass `poolname` parameter, if the SC still exists and
/// carries one.
async fn sc_poolname(ctx: &Ctx, pv: &PersistentVolume) -> Result<Option<String>> {
    let Some(sc_name) = pv.spec.as_ref().and_then(|s| s.storage_class_name.clone()) else {
        return Ok(None);
    };
    let scs: Api<StorageClass> = Api::all(ctx.client.clone());
    let Some(sc) = scs.get_opt(&sc_name).await? else {
        return Ok(None);
    };
    Ok(sc.parameters.and_then(|p| p.get("poolname").cloned()))
}

fn capacity_of_evac(e: &ZFSEvacuation) -> u128 {
    e.status
        .as_ref()
        .and_then(|s| s.source.as_ref())
        .map(|s| s.capacity_bytes as u128)
        .unwrap_or(0)
}

/// If the PV's StorageClass constrains allowedTopologies on openebs.io/nodeid,
/// return the allowed id set; None = unconstrained.
async fn allowed_node_ids(
    ctx: &Ctx,
    pv: &PersistentVolume,
) -> Result<Option<std::collections::HashSet<String>>> {
    let Some(sc_name) = pv.spec.as_ref().and_then(|s| s.storage_class_name.clone()) else {
        return Ok(None);
    };
    let scs: Api<StorageClass> = Api::all(ctx.client.clone());
    let Some(sc) = scs.get_opt(&sc_name).await? else {
        return Ok(None);
    };
    let Some(topos) = sc.allowed_topologies else {
        return Ok(None);
    };
    let mut ids = std::collections::HashSet::new();
    let mut constrained = false;
    for t in topos {
        for expr in t.match_label_expressions.unwrap_or_default() {
            if expr.key == NODE_ID_TOPOLOGY_KEY {
                constrained = true;
                ids.extend(expr.values);
            }
        }
    }
    if !constrained {
        return Ok(None);
    }
    if ids.is_empty() {
        bail!("StorageClass {sc_name} allowedTopologies excludes every node");
    }
    Ok(Some(ids))
}
