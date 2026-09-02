//! The part of pod placement a target selection can honour without the
//! scheduler: nodeSelector, *required* node affinity, and tolerations against
//! hard taints. Resource fit is deliberately not modelled — a pod left
//! Pending on a Ready target for lack of CPU is visible and fixable, its
//! data no worse off than before the move.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Node, NodeSelectorRequirement, NodeSelectorTerm, Pod, Taint, Toleration,
};
use kube::ResourceExt;

#[derive(Clone, Debug, Default)]
pub struct Placement {
    node_selector: BTreeMap<String, String>,
    /// requiredDuringSchedulingIgnoredDuringExecution terms (OR-ed).
    terms: Vec<NodeSelectorTerm>,
    tolerations: Vec<Toleration>,
}

impl Placement {
    pub fn from_pod(pod: &Pod) -> Self {
        let spec = pod.spec.clone().unwrap_or_default();
        Self {
            node_selector: spec.node_selector.unwrap_or_default(),
            terms: spec
                .affinity
                .and_then(|a| a.node_affinity)
                .and_then(|na| na.required_during_scheduling_ignored_during_execution)
                .map(|s| s.node_selector_terms)
                .unwrap_or_default(),
            tolerations: spec.tolerations.unwrap_or_default(),
        }
    }

    /// Would the scheduler consider this node for the pod, as far as labels,
    /// fields and taints go?
    pub fn admits(&self, node: &Node) -> bool {
        let labels = node.labels();
        if self
            .node_selector
            .iter()
            .any(|(k, v)| labels.get(k) != Some(v))
        {
            return false;
        }
        if !self.terms.is_empty() && !self.terms.iter().any(|t| term_matches(t, node)) {
            return false;
        }
        let taints = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.clone())
            .unwrap_or_default();
        taints
            .iter()
            .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
            .all(|t| self.tolerations.iter().any(|tol| tolerates(tol, t)))
    }
}

/// A term matches when every one of its expressions does (an empty term
/// matches nothing, as in the scheduler).
fn term_matches(term: &NodeSelectorTerm, node: &Node) -> bool {
    let exprs = term.match_expressions.clone().unwrap_or_default();
    let fields = term.match_fields.clone().unwrap_or_default();
    if exprs.is_empty() && fields.is_empty() {
        return false;
    }
    let labels = node.labels();
    exprs
        .iter()
        .all(|e| requirement_matches(e, labels.get(&e.key).map(String::as_str)))
        && fields.iter().all(|f| {
            // metadata.name is the only field selector the scheduler supports.
            let value = (f.key == "metadata.name").then(|| node.name_any());
            requirement_matches(f, value.as_deref())
        })
}

fn requirement_matches(req: &NodeSelectorRequirement, value: Option<&str>) -> bool {
    let values = req.values.clone().unwrap_or_default();
    match req.operator.as_str() {
        "In" => value.is_some_and(|v| values.iter().any(|x| x == v)),
        "NotIn" => !value.is_some_and(|v| values.iter().any(|x| x == v)),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" | "Lt" => {
            let (Some(v), Some(bound)) = (
                value.and_then(|v| v.parse::<i64>().ok()),
                values.first().and_then(|b| b.parse::<i64>().ok()),
            ) else {
                return false;
            };
            if req.operator == "Gt" { v > bound } else { v < bound }
        }
        // An operator we do not know cannot exclude a node we would
        // otherwise pick; the scheduler has the last word anyway.
        _ => true,
    }
}

/// The scheduler's toleration semantics: empty key (with Exists) tolerates
/// every taint, empty effect every effect; Equal compares values.
pub fn tolerates(tol: &Toleration, taint: &Taint) -> bool {
    let key_ok = match tol.key.as_deref() {
        None | Some("") => tol.operator.as_deref() == Some("Exists"),
        Some(k) => k == taint.key,
    };
    let effect_ok = match tol.effect.as_deref() {
        None | Some("") => true,
        Some(e) => e == taint.effect,
    };
    let value_ok = match tol.operator.as_deref() {
        Some("Exists") => true,
        _ => tol.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or(""),
    };
    key_ok && effect_ok && value_ok
}
