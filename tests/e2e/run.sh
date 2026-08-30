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

cleanup() { kubectl delete ns "$NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

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

log "ALL PASS"
