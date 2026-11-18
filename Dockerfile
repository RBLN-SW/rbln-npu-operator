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

FROM redhat/ubi9-micro:9.9 AS runtime-base
FROM redhat/ubi9:9.9 AS runtime-packages
COPY --from=runtime-base /var/lib/rpm/ /mnt/rootfs/var/lib/rpm/
RUN dnf -y --installroot=/mnt/rootfs --releasever=9 --setopt=install_weak_deps=False \
    --downloadonly --destdir=/rpms install ca-certificates util-linux-core

FROM runtime-base
# Run RPM scriptlets in the target image itself so buildx can emulate them.
# The mounted package manager and downloaded RPMs stay out of the image.
RUN --mount=from=runtime-packages,target=/build-tools \
    LD_LIBRARY_PATH=/build-tools/usr/lib64 RPM_CONFIGDIR=/build-tools/usr/lib/rpm \
    /build-tools/usr/bin/rpm -U --excludedocs /build-tools/rpms/*.rpm
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
