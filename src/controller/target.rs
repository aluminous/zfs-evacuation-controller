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

pub async fn select_target(ctx: &Ctx, input: &SelectionInput<'_>) -> Result<TargetInfo> {
    let zfsnodes: Api<ZFSNode> = ctx.openebs();
    let nodes = ctx.nodes().list(&ListParams::default()).await?;
    let evacs: Api<ZFSEvacuation> = Api::all(ctx.client.clone());
    let all_evacs = evacs.list(&ListParams::default()).await?;

    let wanted_pool = input
        .spec
        .target_pool
        .clone()
        .unwrap_or_else(|| input.source_pool.to_string());
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
            // Reserve the evacuating volume's capacity against the target pool.
            *reserved
                .entry((t.node_id.clone(), t.pool.clone()))
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
        if busy_targets.contains(&node_id) {
            continue;
        }
        for pool in &zn.pools {
            if pool.name != wanted_pool {
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
    let (node, node_id, pool, _) = candidates.into_iter().next().ok_or_else(|| {
        anyhow!(
            "no eligible target: need {need} bytes in pool {wanted_pool} on a ready node \
             (excluding source {})",
            input.source_node_id
        )
    })?;

    Ok(TargetInfo {
        node,
        node_id,
        pool,
        new_volume_handle: String::new(), // filled by caller
    })
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
