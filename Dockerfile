# Build the manager binary
ARG GOLANG_VERSION=1.25.7

FROM harbor.k8s.rebellions.in/dockerhub-proxy/golang:${GOLANG_VERSION} AS builder
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

RUN make cmds

FROM harbor.k8s.rebellions.in/dockerhub-proxy/redhat/ubi9-minimal:9.6
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

RUN microdnf install -y util-linux-core && microdnf clean all -y

COPY --from=builder /workspace/npu-operator /usr/bin/
COPY --from=builder /workspace/rbln-validator /usr/bin/
COPY LICENSE /licenses/LICENSE

USER 65532:65532

ENTRYPOINT ["/usr/bin/npu-operator"]
