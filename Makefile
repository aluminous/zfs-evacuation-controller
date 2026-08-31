IMAGE ?= ghcr.io/aluminous/zfs-evacuation-controller:latest
DOCKER ?= podman

.PHONY: test check crds image deploy

test:
	cargo test

check:
	cargo clippy -- -D warnings

crds:
	cargo run --quiet -- --print-crds > deploy/crds.yaml

# Cluster is x86_64; build stage cross-compiles natively (no QEMU rustc).
image:
	$(DOCKER) build --platform=linux/amd64 -t $(IMAGE) .

deploy:
	kubectl apply -f deploy/crds.yaml -f deploy/rbac.yaml \
	  -f deploy/admission.yaml -f deploy/controller.yaml
