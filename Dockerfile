# Build the manager binary
ARG GOLANG_VERSION=1.25.7

FROM golang:${GOLANG_VERSION} AS builder
ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /workspace

# Copy build configuration files
COPY Makefile Makefile
COPY versions.mk versions.mk

# Copy Go module files and vendor directory
COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/

# Copy source code
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

# Build the binaries using Makefile
RUN make cmds

# Use distroless as minimal base image to package the npu-operator-manager binary
FROM redhat/ubi9-minimal:9.6
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
COPY LICENSE /licenses/LICENSE

USER 65532:65532

ENTRYPOINT ["/usr/bin/npu-operator"]
