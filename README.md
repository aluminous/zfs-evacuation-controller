# zfs-evacuation-controller

Migrates [openebs zfs-localpv](https://github.com/openebs/zfs-localpv) volumes
to another node ahead of node removal. zfs-localpv PVs are pinned to one node
by immutable `nodeAffinity`; upstream has no migration story
(openebs/zfs-localpv#291, #469). This controller moves the dataset with
`zfs send`/`zfs recv` and recreates the PV **with the same name**. The PVC and
everything referencing it stay untouched.

## How it works

- **Unprivileged.** No daemonset, no zfs-executing code. The actual
  `zfs send | nc` and `nc | zfs recv` are performed by zfs-localpv's own node
  agents, driven through its core `ZFSBackup`/`ZFSRestore` CRs. Both agents
  dial out; the controller runs a per-attempt TCP relay (two peer-IP-pinned,
  single-accept listeners) that splices the stream.
- **Fresh destination identity.** The data lands in a dataset with a new
  unique name (new `volumeHandle`, e.g. `pvc-xxx-e1a2b3`), so a same-name
  dataset on the target is never touched. The PV name never changes.
- **Dataset adoption.** After the receive, the controller creates a ZFSVolume
  CR for the new dataset; the target node agent adopts the pre-existing
  dataset and marks it Ready (the same mechanism zfs-localpv's Velero restore
  relies on).
- **Attach lock.** A `ValidatingAdmissionPolicy` (k8s ≥ 1.30) guards pod
  creation against PVCs under evacuation, including generic ephemeral
  volumes. The locked-PVC sets are delivered via a `paramKind` object
  maintained by the controller (CEL cannot look up PVCs), one list per
  evacuation mode: `pvcKeys` (WhenIdle) denies every referencing pod;
  `claimablePvcKeys` (WhenClaimed) denies only pods that would bypass the
  source node's cordon (`spec.nodeName`, or a toleration for
  `node.kubernetes.io/unschedulable`).
- **Co-location (WhenClaimed).** Volumes are moved with their consumer: the
  Pending pod left behind by a drain names the group (every ZFS PVC it
  references), the group's members pick one target under the pod's own
  placement constraints, and the same Pending pod binds the relocated volumes
  after the swap. kube-scheduler requeues it on the PV update, so nothing is
  deleted or recreated.
- **PV swap.** PV `nodeAffinity` is immutable, so the PV is deleted (its
  `pv-protection` finalizer stripped after quiescence is verified) and
  recreated with the same name and a `claimRef` carrying the PVC's UID. The PV
  controller rebinds the PVC automatically. Both manifests are stored in
  the ZFSEvacuation status before the delete, so a
  controller crash mid-swap always resumes.
- **Safety rails.** The source ZFSVolume gets a guard finalizer (the node
  agent won't destroy while it's present) and the PV is flipped to `Retain`
  before anything else happens; the new PV is created as `Retain` and only
  flipped back to the original policy after it is Bound. The source dataset is
  destroyed (by deleting its ZFSVolume CR) only after the swap is verified.

## Usage

```sh
kubectl apply -f deploy/crds.yaml -f deploy/rbac.yaml \
  -f deploy/admission.yaml -f deploy/controller.yaml

# Trigger 1: annotate a PV to evacuate that one volume.
kubectl annotate pv pvc-1234... zfsevac.alumino.us/evacuate=true

# Trigger 2: taint a node to evacuate ALL its zfs-localpv volumes. Use a
# hard effect (NoSchedule/NoExecute): taint-triggered evacuations refuse to
# copy while the source can still receive pods, and a hard taint satisfies
# that by itself (so does kubectl cordon/drain). A softer effect triggers
# too, but everything holds until the node repels pods. A tainted node is
# also excluded as an evacuation target.
kubectl taint node worker-3 zfsevac.alumino.us/evacuate=:NoSchedule

kubectl get zfsevacuations        # short name: zevac
```

Both triggers **lock the PVC immediately** (attach lock, see above), in use
or not. Nothing is evicted by the controller. Drain the node yourself, or
let the workload finish. The order matters: trigger first, evict second.

An evacuation runs in one of two modes (`spec.mode`, read once when the lock
is taken):

- **WhenIdle** — the annotation trigger. The transfer starts once the last
  pod referencing the PVC is gone, and *no* pod may be created against it
  until the volume has moved. A Deployment/StatefulSet replacement is denied
  at creation and its controller retries until the swap, at which point the
  pod lands on the new node. Volumes are placed independently. This emergency
  mode ignores co-location and does not require a cordon.
- **WhenClaimed** — the taint trigger, always. The intended sequence is
  taint (hard effect), then `kubectl drain`: the evicted consumer's
  replacement is created (the lock allows scheduler-routed pods) and stays
  Pending, because PV affinity pins it to a node that repels pods. That
  Pending pod defines the group. All of its ZFS PVCs move to one node that
  satisfies the pod's nodeSelector/affinity/tolerations and has room for the
  lot (`status.colocation` records the pod, members, and which member led
  the placement). Once the PVs are swapped, the scheduler binds the same
  pod there. A volume with no consumer resolves no group and is placed on
  its own. The source must repel pods (cordon, or the evacuate taint with a
  NoSchedule/NoExecute effect) from before the copy to completion, **whether
  or not any consumer exists**. The lock admits scheduler-routed pods, so
  only the node's own state keeps a newly created consumer off the source
  mid-copy. The evacuation holds in Quiescing/Committing/Swapping with a
  `status.message` asking for the cordon otherwise. Removing the taint
  cancels, as always.

Do not trigger what you do not intend to drain: while a WhenIdle lock holds,
a crashed consumer cannot restart until the migration completes.

Remove the annotation or delete the ZFSEvacuation to cancel. Cancellation is
honored any time before the commit point (the old-PV delete); after that, the
machine rolls forward only.

Optional per-evacuation tuning on the ZFSEvacuation spec: `mode`,
`targetNode` (under WhenClaimed it also anchors the pod's other volumes),
`targetPool`, `settleSeconds`, `transferTimeoutSeconds`, `maxAttempts`,
`headroomPercent`.

## State machine

Pending → Guarding → Retaining → Locking → Quiescing → TargetSelecting →
Transferring → Adopting → Committing → **(commit point)** → Swapping →
CleaningUp → Completed, with Aborting/Failed reachable before the commit
point. Every phase is idempotent; intent is written to status before the
corresponding external mutation.

## Preconditions / phase-1 scope

Refused up front: volumes with ZFSSnapshots, clones, `marked-for-deletion`,
or an in-flight resize; volumes whose source node is already gone (the data
is unreachable; restore from backup instead). One transfer per source and
per target node at a time; full (non-incremental) send per attempt.

Destination poolname: `spec.targetPool` verbatim if set; else the PV's
StorageClass `poolname` parameter (what provisioning on the target would
have used); else the source volume's poolName carried over. Poolnames may be
dataset paths ("zroot/csi"). Target eligibility and capacity are judged on
the zpool component, and if the parent dataset of the destination does not
exist on the target, the restore fails there: provisioning parent datasets
on every node is the operator's contract (the controller is unprivileged and
cannot create or probe them).

Cancel semantics per trigger: annotation evacuations cancel when the
annotation is removed; taint evacuations cancel when the source node's taint
is removed (a *deleted* node is not a cancel). The taint key defaults to
`zfsevac.alumino.us/evacuate` (env `EVACUATE_TAINT_KEY`).

## Operational notes

- The old `volumeHandle` changes: anything keyed on it (external backup
  tooling, Velero ZFSBackup references) must be re-pointed after evacuation.
- If completed Job pods reference the PVC, evacuation waits. Pod *objects*
  are the only unmount signal available without a privileged agent. Use
  `ttlSecondsAfterFinished`.
- Quiescence is "no pod objects + settle period" (default 90 s): the guarantee
  is crash-consistency in the worst case; the VAP lock prevents any writer
  from re-attaching mid-copy.
- Transfer sequencing: each attempt creates the ZFSBackup first and the
  ZFSRestore only once the source has connected to the relay, which buffers
  the send stream (up to 64 MiB) until the target arrives. This matters
  because the zfs-localpv agents run `nc -w 3`, where `-w` is an *idle*
  timeout: a restore that connects before the source has produced bytes dies
  after exactly 3 s, and so does either side of a stream that stalls for 3 s
  mid-flight (relayed/DERP paths between nodes do this routinely; give nodes
  a direct path). `transferTimeoutSeconds` (default 3600) bounds the wait for
  the source, the wait for the target and a stalled stream, but never kills a
  stream that is still moving bytes; a stall longer than
  `TRANSFER_STALL_SECONDS` (default 120) fails the attempt. Up to
  `maxAttempts` (default 3) per evacuation; the reason each attempt failed is
  recorded in `status.transfer.failureReason` and in the events.
- The relay is cleartext TCP; its access control is the runtime peer
  allowlist, derived live from the Node objects (node addresses + pod CIDRs),
  scoped per attempt to the two expected nodes, single-accept, on random
  ports. No NetworkPolicy is shipped: it would hardcode
  site CIDRs into a manifest and add nothing the pinning doesn't already do
  more precisely. zfs send streams are internally checksummed; recv fails
  loudly on corruption.
- If a node is deleted while CRs still reference it, the controller strips
  the stuck `zfs.openebs.io/finalizer` itself. The disk left with the node.
- The transfer snapshot exists twice: `zfs send` needs one on the source
  (`@zevac-a<attempt>-<hex>`, made by the ZFSBackup and destroyed by its
  finalizer), and the received stream recreates it on the target, where
  nothing in zfs-localpv knows about it. CleaningUp destroys the target copy
  through the target's node agent. It registers a ZFSSnapshot CR for it
  (the agent's create is a no-op on an existing snapshot) and deletes the CR
  so the agent's finalizer runs `zfs destroy`. This runs before the source
  dataset is destroyed, so a target agent that never answers leaves both
  copies intact rather than a snapshot that silently pins every block the
  volume later overwrites.

## Development

```sh
cargo test                      # state-machine/pure-logic unit tests
cargo run -- --print-crds       # regenerate deploy/crds.yaml
tests/e2e/run.sh                # end-to-end (needs two-node ZFS cluster, see below)
```

End-to-end testing needs real ZFS nodes (kind/k3d on macOS can't provide
them): two Linux VMs (lima/multipass) running k3s with file-backed zpools and
zfs-localpv installed. See `tests/e2e/README.md`.
