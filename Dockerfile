# Cross-compiling multi-stage build: the build stage runs on the host's
# native architecture (fast — no QEMU-emulated rustc) and cross-compiles to
# x86_64; the runtime image is linux/amd64 for the cluster.
#
#   docker build --platform=linux/amd64 -t zfs-evacuation-controller .
FROM --platform=$BUILDPLATFORM rust:1.95-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-gnu
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-gnu

FROM --platform=linux/amd64 gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/zfs-evacuation-controller /usr/local/bin/zfs-evacuation-controller
USER nonroot
ENTRYPOINT ["/usr/local/bin/zfs-evacuation-controller"]
