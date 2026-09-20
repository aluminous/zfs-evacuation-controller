# Co-locating a consumer's volumes: three approaches, researched

Status: B implemented, 2026-09-03 (`spec.mode`, `status.colocation`,
`claimablePvcKeys` in EvacuationParams. The list is named
`pvcKeysSchedulerRouted` below; README.md is the current description).
A and C are not implemented.
Supersedes the pod-graph proposal of 2026-09-02 (which needed a pod to
exist at lock time to learn the group; that objection prompted this proposal).

## The problem, in one paragraph

Target selection is per volume, and "most free space" is a balancer:
every landed volume makes its node less attractive for the next, so the
volumes one pod mounts together are actively pushed apart whenever two
targets are close in free space. hermes-dev's three PVCs split 2/1 in
rotation 3 and its pod was unschedulable (`volume node affinity
conflict`). Whatever fixes this has to answer two questions: *which
volumes belong together*, and *how does the consumer find them again*.

## A. Group label only

Every ZFS PVC carries `zfsevac.alumino.us/group: <id>`, set by the user
or stamped automatically; evacuation refuses to move a partial group;
selection is leader/follower on the label.

**Automatic stamping is well-defined.** Both ZFS StorageClasses are
`WaitForFirstConsumer`, so a PVC binds exactly when the scheduler places
its first consumer. The pod exists, is scheduled, and its full volume
list is known at that moment. The stamper is a pod watch: whenever a pod
is scheduled, label every ZFS PVC it mounts with the same group id. The
id should be a content hash of the sorted PVC names, not the pod hash:
`pod-template-hash` changes on every Deployment rollout and the pod UID
on every restart, which would relabel the same volumes forever; a
content hash is idempotent (no write when nothing changed) and two pods
mounting the same set agree without coordination. Pods sharing only
*some* PVCs (P1: A,B; P2: B,C) are the one ambiguity. Last writer wins, and
the case is rare enough to document rather than solve.

**What it needs beyond the previous proposal:** the stamper (pod watch,
PVC patch; RBAC is already present, and the controller already labels PVCs
with `evacuating`), a startup backfill from currently-running pods
(production's volumes bound long before the controller existed), and the
"partial group" refusal in Pending for Annotation-triggered evacuations
(NodeTaint annotates the whole node, so groups are complete by
construction; a sibling that already lives elsewhere is handled by the
follow rule below). ArgoCD tolerates controller-added labels on managed
PVCs; the `evacuating` label proves it.

**What it does not solve.** Selection still runs before any consumer
exists, so it cannot know whether the group's node is one the consumer
can use (nodeSelector, taints, resources); it only guarantees
"together". And the workload still has to come back through the attach
lock: the replacement pod is denied at creation until the swap, so the
ReplicaSet create-backoff (~10 min for hermes-dev, ~8 min for KubeVirt)
stays part of every rotation's downtime.

## B. Two modes: when-idle and when-claimed

*when-idle* is today's behaviour. *when-claimed* watches for pods that
are Pending because their volumes' node is tainted, moves those volumes
as a group, and lets the scheduler bind the pod to the relocated
volumes. The question was whether the pending pod can pick up the
relocated volume without being deleted. It can, and the mechanism is
already the one the controller relies on.

**How a pending pod finds the relocated volume.** The swap already
recreates the PV under the same name with a `claimRef` carrying the
PVC's UID; the PV controller re-binds and the PVC goes back to `Bound`
(this is how every production evacuation has completed; the PVC shows
`Lost` for the seconds between delete and create). On the scheduler
side, the VolumeBinding plugin registers `PersistentVolume Add|Update`
and `PersistentVolumeClaim Add|Update` as cluster events; a pending pod
that was rejected by VolumeBinding is re-queued on any PV add/update,
and on PVC updates that touch one of its own claims
([volume_binding.go][vb]). Queueing hints are GA and locked on since
Kubernetes 1.34 ([scheduling framework][sf]); production runs 1.36.1.
So the sequence is: PV deleted → pod retried, still unschedulable (claim
Lost) → PV created → PVC Bound → pod retried → Filter passes on the
target → Bind. Retry latency is the scheduler backoff, ≤ 10 s. The
Adopting phase (target ZFSVolume Ready) precedes the swap, so the
kubelet never races the dataset.

**No deletion, and no create-backoff either.** The pod object persists
throughout; nothing is recreated, so the ReplicaSet/KubeVirt backoff that
today adds ~10 min after the lock lifts disappears. Downtime becomes
drain-to-swap, i.e. the transfer itself.

**What replaces the attach lock's job.** The lock exists so a
replacement pod cannot attach to the source mid-copy. Under
when-claimed the replacement is *wanted* as a Pending object, so
creation must be allowed for scheduler-routed pods. What keeps them off
the source is that the source is unschedulable: `kubectl drain` cordons
it, and cordon is enforced by the scheduler for everything without a
`node.kubernetes.io/unschedulable` toleration. (The evacuate taint is
`PreferNoSchedule`, so it contributes nothing here; the cordon does the
work.) The one thing cordon does not stop is a pod created with
`spec.nodeName` set. Those bypass the scheduler entirely, and that is
exactly what the VAP should keep denying. Concretely: the policy's deny
expression gains `&& has(object.spec.nodeName) && object.spec.nodeName
!= ''` for keys in a new `params.spec.pvcKeysSchedulerRouted` list;
`pvcKeys` keeps today's semantics for when-idle (annotation trigger, no
cordon). Quiescing changes accordingly: never-scheduled pods do not block
*if* the source node is `unschedulable` (or carries a NoSchedule taint);
otherwise they block as today, with the existing message.

**Group = the pending pod's ZFS PVCs.** This is the group definition
that needs no bookkeeping, no labels and no pod at lock time: the pod
that will consume the volumes is the pod that defines the set, and it
exists precisely when the set matters. Selection for the group is the
leader/follower mechanism from the earlier proposal, minus discovery:
the first of the pod's volumes to select takes the summed
capacity+headroom of all of them and reserves the rest on its target;
the others follow any sibling that already has a home (`status.target`
of a sibling's CR, or the sibling's ZFSVolume `ownerNodeID` off the
source). Idle volumes on the tainted node have no consumers, so they take the
when-idle path unchanged. There is nothing to
co-locate them *with*. A volume referenced by a still-running pod waits
for the drain to evict it, as today.

**Selecting a node the pod can use.** Because the pod exists, selection
can respect the pod's own placement constraints cheaply: required
nodeAffinity/nodeSelector, and taints vs tolerations, are pure label/
taint matching; resource fit is not (it needs the scheduler's
accounting) and is skipped. A pod that stays Pending on a
Ready target for resource reasons is visible and operator-fixable, and
its data is no worse off than today. This is the cluster-autoscaler
posture: it too acts on Pending pods and simulates scheduling against
the pod's constraints rather than guessing ([CA FAQ, "How does scale-up
work?"][ca]). That is the precedent the mode follows. Where CA answers
"add a node this pod fits", when-claimed answers "move this pod's
volumes to a node it fits".

**Additional benefit: split repair.** A pod whose volumes already sit on two
different nodes is Pending forever with the same affinity conflict, and
it is *intrinsically* unschedulable everywhere (it needs all its PVs on
one node), so moving its minority volume is safe without any cordon.
The same watcher, triggered by the conflict rather than by a taint,
would have repaired hermes-dev on its own. It is outside the first cut.

**What changes in clusterctl.** The wait-for-locks step before the
drain goes away: the order becomes taint → drain → wait for the node to
own no ZFSVolumes. The drain's evictions produce the Pending pods that
drive the evacuations.

**Risks found.** (1) Anything tolerating `node.kubernetes.io/
unschedulable` (DaemonSets do, workloads normally don't) could still be
scheduled to the cordoned source: the VAP can additionally deny pods
carrying that toleration for locked keys. (2) Annotation-triggered
evacuations get nothing from this mode unless the operator cordons the
node; they stay when-idle. (3) A Pending pod that is unschedulable for
an unrelated reason (resources) would trigger an evacuation of volumes
that a same-node reschedule could have served, but only when the node is
tainted, i.e. the volumes have to leave anyway. No real cost.

## C. Operator picks the node by annotation

A PVC annotation `zfsevac.alumino.us/target-node` that the trigger loop
copies into `spec.targetNode`. Everything else exists: explicit targets
already override selection, and rule "follow a sibling with a home"
would let one annotated PVC anchor its siblings if that rule is added.

**Findings.** It is a two-line change in `trigger.rs` and turns
hermes-dev's five-step manual repair into one `kubectl annotate` *before*
the rotation. It also has to be applied before the taint (the trigger
creates CRs within 15 s of the taint), and it is opt-in: the default
behaviour still splits, so the operator has to remember, per workload,
per rotation. As the only mechanism it is a footgun; as the override for
either A or B it is useful.

## Recommendation

**B, with C as the override.** B is the only option that gets the group
definition from the consumer that needs co-location, keeps the
scheduler in charge of placement (including the pod's own constraints),
and removes the create-backoff from rotation downtime as a side effect.
It also shrinks the design: no stamper, no labels to keep fresh, no pod
graph at lock time, and the attach lock narrows to the one case the
cordon cannot cover. A holds up technically and is the fallback if some
consumer turns out never to produce a Pending pod. A bare pod with no
controller is the example, and for that the volume is simply idle after
the drain and takes the when-idle path, which is correct.

Order of work: C first (trivial, immediately useful), then B's core
(VAP narrowing + quiescing rule + pending-pod grouping + leader/follow
selection + clusterctl ordering), then split repair.

### B, concretely

```yaml
# ZFSEvacuation
spec:
  mode: WhenClaimed | WhenIdle   # trigger loop: WhenClaimed for NodeTaint,
                                 # WhenIdle for Annotation; settable by hand
status:
  colocation:
    pod: hermes-dev/hermes-dev-7c9f4-x2k8q   # the Pending consumer
    members: [{pvName, capacityBytes}, …]     # its ZFS PVCs
    role: Leader | Follower
    followed: pvc-…                           # Follower: the anchor
    constraints: [nodeSelector, tolerations]  # what selection honoured

# EvacuationParams
spec:
  pvcKeys: [...]                 # deny all creation (WhenIdle)
  pvcKeysSchedulerRouted: [...]  # deny only spec.nodeName pods (WhenClaimed)
```

Phases: unchanged names. Locking adds the key to the list matching the
mode. Quiescing: scheduled pods block; never-scheduled pods block only
when the source is schedulable. TargetSelecting: if a never-scheduled
pod references the PVC, the group is that pod's ZFS PVCs, and the
candidate filter adds the pod's node constraints; leader/follow as
above; with `spec.targetNode` the follow rule anchors the siblings.
Everything from Transferring on is untouched.

Tests: pending pod + cordoned source → quiescing proceeds; pending pod +
schedulable source → blocks with message; leader sums the group and
reserves; follower follows `status.target` and ZFSVolume owner;
nodeSelector excludes a candidate; VAP unit cases (nodeName set vs
empty, both lists). e2e: two-PVC Deployment, rotate its node with two
near-equal targets, assert both ZFSVolumes share an owner and that the
*same pod object* (UID) goes Running on it. The UID assertion is what
proves no delete/recreate happened.

[vb]: https://github.com/kubernetes/kubernetes/blob/release-1.36/pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
[sf]: https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/
[ca]: https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md
