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
///  1. spec.targetPool verbatim (operator override);
///  2. the PV's StorageClass `poolname` parameter: by definition what
///     provisioning this PVC on the target node would have used;
///  3. the source volume's poolName carried over (SC gone or missing the
///     parameter).
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
    let need = input.capacity_bytes + input.capacity_bytes * headroom / 100;

    // Reservation ledger + set of nodes already involved in an evacuation
    // (concurrency cap: 1 inbound + 1 outbound per node).
    let mut reserved: std::collections::HashMap<(String, String), u128> = Default::default();
    let mut busy_targets: std::collections::HashSet<String> = Default::default();
    for e in &all_evacs {
        if e.name_any() == input.evac_name {
            continue;
        }
        let Some(st) = &e.status else { continue };
        if matches!(st.phase, Phase::Completed | Phase::Failed) {
            continue;
        }
        if let Some(t) = &st.target {
            busy_targets.insert(t.node_id.clone());
            // Reserve the evacuating volume's capacity against the target's
            // zpool. status.target.pool may be a dataset path; capacity is a
            // per-zpool quantity, so the ledger keys on the pool component.
            *reserved
                .entry((t.node_id.clone(), pool_component(&t.pool).to_string()))
                .or_default() += capacity_of_evac(e);
        }
    }

    // Allowed node ids from the StorageClass topology, if constrained.
    let allowed_ids = allowed_node_ids(ctx, input.pv).await?;

    let mut candidates = Vec::new();
    for zn in zfsnodes.list(&ListParams::default()).await? {
        let node_id = zn.name_any();
        if node_id == input.source_node_id {
            continue;
        }
        if let Some(explicit) = &input.spec.target_node {
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
        if let Some(allowed) = &allowed_ids
            && !allowed.contains(&node_id) {
                continue;
            }
        let Some(node) = nodes.iter().find(|n| node_id_of(n) == node_id) else {
            continue;
        };
        if !node_is_ready(node) {
            continue;
        }
        // A node marked for evacuation must never receive evacuated data.
        if crate::controller::node_has_evacuate_taint(node, &ctx.cfg.taint_key) {
            continue;
        }
        if busy_targets.contains(&node_id) {
            continue;
        }
        for pool in &zn.pools {
            // ZFSNode reports bare zpool names; the destination may be a
            // dataset path within one. Eligibility is a zpool property.
            if pool.name != wanted_component {
                continue;
            }
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
            }
        }
    }

    // Most free space first.
    candidates.sort_by_key(|c| std::cmp::Reverse(c.3));
    let (node, node_id, _, _) = candidates.into_iter().next().ok_or_else(|| {
        anyhow!(
            "no eligible target: need {need} bytes in zpool {wanted_component} \
             (for destination {dest_pool}) on a ready node (excluding source {})",
            input.source_node_id
        )
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
