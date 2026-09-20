# End-to-end test environment

The e2e suite needs a two-node Kubernetes cluster with ZFS on both nodes.
macOS containers cannot load the ZFS kernel module, so run it in VMs.

## One-time setup (lima)

```sh
# Two Ubuntu VMs
limactl start --name=zfs1 template://k3s
limactl start --name=zfs2 template://ubuntu   # joined as agent, see below

# In each VM: zfs + a file-backed pool
sudo apt-get install -y zfsutils-linux
sudo truncate -s 10G /var/lib/zpool.img
sudo zpool create tank /var/lib/zpool.img

# Join zfs2 to the zfs1 k3s server (K3S_URL/K3S_TOKEN), then install zfs-localpv:
kubectl apply -f https://openebs.github.io/charts/zfs-operator.yaml

# StorageClass:
kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: { name: zfs }
provisioner: zfs.csi.openebs.io
parameters: { poolname: tank, fstype: zfs }
EOF
```

Then deploy the controller. Build its image with the Dockerfile, import it into
k3s, and run `./run.sh`.

## What run.sh covers

1. Happy path: PVC + data written → pod removed → PV annotated → asserts
   data present on target node, PVC Bound to same PV name with new
   volumeHandle, source dataset destroyed.
2. Attach lock: pod creation referencing the PVC is denied mid-evacuation.
3. Collision: dataset with the same name pre-created on the target is
   untouched (new handle used).
4. Cancel: annotation removed during Transferring → clean unwind, original
   PV/policy intact.
5. Abort on PVC deletion mid-transfer.
6. Crash recovery: controller killed during Swapping → resumes and completes.
7. Node deletion before CleaningUp → stuck finalizers stripped, CR unstuck.
