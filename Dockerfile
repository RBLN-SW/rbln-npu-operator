# Build the manager binary
ARG GOLANG_VERSION=1.26.10

# The builder always runs on the build host (BUILDPLATFORM) and cross-compiles
# for TARGETARCH: Go needs no emulation (CGO_ENABLED=0), so a multi-arch
# build only emulates the small runtime stage below.
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION} AS builder
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

COPY Makefile Makefile
COPY versions.mk versions.mk

COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

RUN make cmds GOOS=$TARGETOS GOARCH=$TARGETARCH

# Install runtime RPMs in a UBI Micro filesystem, keeping the package
# manager and its networking dependencies in the build stage.
FROM redhat/ubi9-micro:9.9 AS runtime-base
FROM redhat/ubi9:9.9 AS runtime-rootfs
COPY --from=runtime-base / /mnt/rootfs/
RUN dnf -y --installroot=/mnt/rootfs --releasever=9 \
        --setopt=install_weak_deps=False --setopt=tsflags=nodocs \
        install ca-certificates util-linux-core && \
    dnf --installroot=/mnt/rootfs clean all

FROM runtime-base
COPY --from=runtime-rootfs /mnt/rootfs/ /
ARG VERSION

LABEL \
    name="rbln-npu-operator" \
    vendor="Rebellions" \
    version="${VERSION}" \
    release="N/A" \
    summary="Deploy and manage Rebellions NPU resources in Kubernetes" \
    description="Rebellions NPU Operator" \
    maintainer="Rebellions sw_devops@rebellions.ai" \
    io.k8s.display-name="Rebellions NPU Operator" \
    com.redhat.component="rbln-npu-operator"

COPY --from=builder /workspace/npu-operator /usr/bin/
COPY --from=builder /workspace/rbln-validator /usr/bin/
COPY --from=builder /workspace/crd-apply /usr/bin/
COPY --from=builder /workspace/rbln-system-probe /usr/bin/
COPY config/crd/bases/ /opt/rbln/crds/
COPY LICENSE /licenses/LICENSE

USER 65532:65532

ENTRYPOINT ["/usr/bin/npu-operator"]
