#!/usr/bin/env bash
# e2e smoke suite. Requires: a two-node cluster with zfs-localpv + this
# controller deployed (see README.md), kubectl context pointing at it, and
# ssh access to the nodes for zfs-level assertions (NODE1_SSH/NODE2_SSH, e.g.
# NODE1_SSH="limactl shell zfs1").
set -euo pipefail

NS=e2e-zevac
SC=${STORAGE_CLASS:-zfs}
NODE1_SSH=${NODE1_SSH:?set NODE1_SSH to a command prefix that runs on node 1}
NODE2_SSH=${NODE2_SSH:?set NODE2_SSH to a command prefix that runs on node 2}

log() { echo "--- $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

wait_for() { # <timeout-secs> <description> <command...>
  local t=$1 desc=$2; shift 2
  for _ in $(seq "$t"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  fail "timed out waiting for: $desc"
}

TAINT=zfsevac.alumino.us/evacuate
TAINTED_NODE=
cleanup() {
  kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ -n "$TAINTED_NODE" ]; then
    kubectl taint node "$TAINTED_NODE" "$TAINT-" >/dev/null 2>&1 || true
    kubectl uncordon "$TAINTED_NODE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

node_name_for_id() { # openebs.io/nodeid -> Node name
  if kubectl get node "$1" -o name >/dev/null 2>&1; then echo "$1"; return; fi
  kubectl get nodes -l "openebs.io/nodeid=$1" -o jsonpath='{.items[0].metadata.name}'
}
pv_node_id() { kubectl get pv "$1" -o jsonpath='{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}'; }
zv_owner() { kubectl get zfsvolume -n openebs "$1" -o jsonpath='{.spec.ownerNodeID}'; }

log "setup: namespace + PVC + writer pod"
kubectl create ns "$NS"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data }
spec:
  storageClassName: $SC
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 1Gi } }
---
apiVersion: v1
kind: Pod
metadata: { name: writer }
spec:
  restartPolicy: Never
  containers:
    - name: w
      image: busybox
      command: ["sh", "-c", "echo canary-\$\$-\$(date +%s) | tee /data/canary && sync && sleep 2"]
      volumeMounts: [{ name: d, mountPath: /data }]
  volumes: [{ name: d, persistentVolumeClaim: { claimName: data } }]
EOF
wait_for 120 "writer pod completion" \
  bash -c "kubectl get pod -n $NS writer -o jsonpath='{.status.phase}' | grep -q Succeeded"
PV=$(kubectl get pvc -n "$NS" data -o jsonpath='{.spec.volumeName}')
OLD_HANDLE=$(kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeHandle}')
# exec into a Succeeded pod always fails — the writer tees the canary to
# stdout so we read it from the logs, and it is mandatory.
CANARY=$(kubectl logs -n "$NS" writer | head -1)
[ -n "$CANARY" ] || fail "could not capture canary from writer logs"
kubectl delete pod -n "$NS" writer --wait

log "test 3 precondition: pre-create a colliding dataset name on both nodes"
$NODE2_SSH sudo zfs create -V 8M "tank/${OLD_HANDLE}-collision" 2>/dev/null || true

log "trigger evacuation of $PV (handle $OLD_HANDLE)"
kubectl annotate pv "$PV" zfsevac.alumino.us/evacuate=true

wait_for 60 "ZFSEvacuation created" kubectl get zfsevacuation "$PV"

log "test 2: attach lock denies pod creation mid-evacuation"
wait_for 300 "lock (past Locking phase)" bash -c \
  "kubectl get zfsevacuation $PV -o jsonpath='{.status.phase}' | grep -Eq 'Quiescing|TargetSelecting|Transferring|Adopting|Committing|Swapping'"
if kubectl run -n "$NS" blocked --image=busybox --restart=Never \
     --overrides="{\"spec\":{\"volumes\":[{\"name\":\"d\",\"persistentVolumeClaim\":{\"claimName\":\"data\"}}],\"containers\":[{\"name\":\"blocked\",\"image\":\"busybox\",\"command\":[\"true\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}]}}" \
     >/dev/null 2>&1; then
  fail "pod referencing evacuating PVC was admitted"
fi
log "attach lock OK (pod creation denied)"

log "test 1: happy path completes"
wait_for 900 "evacuation Completed" bash -c \
  "kubectl get zfsevacuation $PV -o jsonpath='{.status.phase}' | grep -q Completed"

NEW_HANDLE=$(kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeHandle}')
[ "$NEW_HANDLE" != "$OLD_HANDLE" ] || fail "volumeHandle did not change"
[ "$(kubectl get pvc -n $NS data -o jsonpath='{.status.phase}')" = "Bound" ] || fail "PVC not Bound"
[ "$(kubectl get pvc -n $NS data -o jsonpath='{.spec.volumeName}')" = "$PV" ] || fail "PVC volumeName changed"

log "assert source dataset destroyed, target dataset present, collision untouched"
$NODE1_SSH sudo zfs list "tank/$OLD_HANDLE" >/dev/null 2>&1 && fail "source dataset still exists"
$NODE2_SSH sudo zfs list "tank/$NEW_HANDLE" >/dev/null 2>&1 || fail "target dataset missing"
$NODE2_SSH sudo zfs list "tank/${OLD_HANDLE}-collision" >/dev/null 2>&1 || fail "colliding dataset was destroyed"
[ -z "$($NODE2_SSH sudo zfs list -H -t snapshot -o name "tank/$NEW_HANDLE" 2>/dev/null)" ] || fail "transfer snapshot leaked on target"
[ -z "$(kubectl get zfssnapshots -n openebs -l openebs.io/persistent-volume="$NEW_HANDLE" -o name)" ] || fail "cleanup ZFSSnapshot CR left behind"

log "verify data via a reader pod on the new node"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: Pod
metadata: { name: reader }
spec:
  restartPolicy: Never
  containers:
    - name: r
      image: busybox
      command: ["sh", "-c", "cat /data/canary && sleep 2"]
      volumeMounts: [{ name: d, mountPath: /data }]
  volumes: [{ name: d, persistentVolumeClaim: { claimName: data } }]
EOF
wait_for 120 "reader pod completion" \
  bash -c "kubectl get pod -n $NS reader -o jsonpath='{.status.phase}' | grep -q Succeeded"
GOT=$(kubectl logs -n "$NS" reader)
[ "$GOT" = "$CANARY" ] || fail "data mismatch: '$GOT' != '$CANARY'"
kubectl delete pod -n "$NS" reader --wait

# ---------------------------------------------------------------------------
# test 4: WhenClaimed — taint + drain moves a consumer's volumes together and
# the *same* Pending pod binds them on the new node (no delete/recreate).
# ---------------------------------------------------------------------------
NODE2_ID=$(pv_node_id "$PV")
NODE2=$(node_name_for_id "$NODE2_ID")
[ -n "$NODE2" ] || fail "cannot resolve Node for nodeid $NODE2_ID"

log "test 4 setup: Deployment with two ZFS PVCs, co-located on $NODE2"
kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data2 }
spec:
  storageClassName: $SC
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 1Gi } }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: app }
spec:
  replicas: 1
  selector: { matchLabels: { app: app } }
  template:
    metadata: { labels: { app: app } }
    spec:
      containers:
        - name: a
          image: busybox
          command: ["sh", "-c", "cp /data/canary /data2/canary; sync; sleep 3600"]
          volumeMounts:
            - { name: d, mountPath: /data }
            - { name: d2, mountPath: /data2 }
      volumes:
        - { name: d, persistentVolumeClaim: { claimName: data } }
        - { name: d2, persistentVolumeClaim: { claimName: data2 } }
EOF
wait_for 180 "app pod Running" bash -c \
  "kubectl get pod -n $NS -l app=app -o jsonpath='{.items[0].status.phase}' | grep -q Running"
PV2=$(kubectl get pvc -n "$NS" data2 -o jsonpath='{.spec.volumeName}')
[ "$(pv_node_id "$PV2")" = "$NODE2_ID" ] || fail "data2 was not provisioned on $NODE2"
OLD_UID=$(kubectl get pod -n "$NS" -l app=app -o jsonpath='{.items[0].metadata.uid}')

log "test 4: taint $NODE2 (lock), then drain it (consumer goes Pending)"
kubectl taint node "$NODE2" "$TAINT=true:PreferNoSchedule"
TAINTED_NODE=$NODE2
wait_for 60 "both ZFSEvacuations locked (WhenClaimed)" bash -c \
  "[ \"\$(kubectl get zfsevacuation $PV $PV2 -o jsonpath='{range .items[*]}{.spec.mode}/{.status.lockedAt}{\"\n\"}{end}' | grep -c '^WhenClaimed/20')\" = 2 ]"
kubectl drain "$NODE2" --ignore-daemonsets --delete-emptydir-data --timeout=120s
wait_for 60 "replacement pod Pending" bash -c \
  "kubectl get pod -n $NS -l app=app -o jsonpath='{.items[0].status.phase}' | grep -q Pending"
NEW_UID=$(kubectl get pod -n "$NS" -l app=app -o jsonpath='{.items[0].metadata.uid}')
[ "$NEW_UID" != "$OLD_UID" ] || fail "drain did not replace the consumer pod"

log "test 4: cordon bypass denied while the volumes are claimable"
if kubectl run -n "$NS" bypass --image=busybox --restart=Never \
     --overrides="{\"spec\":{\"nodeName\":\"$NODE2\",\"volumes\":[{\"name\":\"d\",\"persistentVolumeClaim\":{\"claimName\":\"data\"}}],\"containers\":[{\"name\":\"b\",\"image\":\"busybox\",\"command\":[\"true\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}]}}" \
     >/dev/null 2>&1; then
  fail "pod with spec.nodeName referencing a claimable PVC was admitted"
fi

log "test 4: both evacuations complete to the same node"
wait_for 1200 "both evacuations Completed" bash -c \
  "[ \"\$(kubectl get zfsevacuation $PV $PV2 -o jsonpath='{range .items[*]}{.status.phase}{\"\n\"}{end}' | grep -c Completed)\" = 2 ]"
T1=$(kubectl get zfsevacuation "$PV" -o jsonpath='{.status.target.nodeId}')
T2=$(kubectl get zfsevacuation "$PV2" -o jsonpath='{.status.target.nodeId}')
[ "$T1" = "$T2" ] || fail "volumes split across nodes: $PV -> $T1, $PV2 -> $T2"
[ "$T1" != "$NODE2_ID" ] || fail "volumes did not leave $NODE2"
ROLES=$(kubectl get zfsevacuation "$PV" "$PV2" -o jsonpath='{range .items[*]}{.status.colocation.role}{" "}{end}')
case "$ROLES" in *Leader*Follower*|*Follower*Leader*) ;; *) fail "unexpected colocation roles: '$ROLES'";; esac

log "test 4: the same Pending pod bound the relocated volumes"
wait_for 180 "app pod Running on $T1" bash -c \
  "kubectl get pod -n $NS -l app=app -o jsonpath='{.items[0].status.phase}' | grep -q Running"
[ "$(kubectl get pod -n "$NS" -l app=app -o jsonpath='{.items[0].metadata.uid}')" = "$NEW_UID" ] \
  || fail "consumer pod was recreated instead of binding in place"
POD_NODE=$(kubectl get pod -n "$NS" -l app=app -o jsonpath='{.items[0].spec.nodeName}')
[ "$POD_NODE" = "$(node_name_for_id "$T1")" ] || fail "pod on $POD_NODE, volumes on $T1"
for h in "$(kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeHandle}')" \
         "$(kubectl get pv "$PV2" -o jsonpath='{.spec.csi.volumeHandle}')"; do
  [ "$(zv_owner "$h")" = "$T1" ] || fail "ZFSVolume $h owned by $(zv_owner "$h"), expected $T1"
done
APP_POD=$(kubectl get pod -n "$NS" -l app=app -o jsonpath='{.items[0].metadata.name}')
[ "$(kubectl exec -n "$NS" "$APP_POD" -- cat /data2/canary)" = "$CANARY" ] || fail "data2 canary mismatch"

kubectl taint node "$NODE2" "$TAINT-"
kubectl uncordon "$NODE2"
TAINTED_NODE=

log "ALL PASS"
