FROM redhat/ubi9:9.8

ARG VERSION=unknown

LABEL \
    name="rbln-vfio-manager" \
    vendor="Rebellions" \
    version="${VERSION}" \
    release="N/A" \
    summary="Binds Rebellions NPU PCI devices to the vfio-pci driver" \
    description="Runtime image for the RBLN VFIO manager DaemonSet" \
    maintainer="Rebellions sw_devops@rebellions.ai" \
    io.k8s.display-name="Rebellions VFIO Manager" \
    com.redhat.component="rbln-vfio-manager" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.source="https://github.com/RBLN-SW/rbln-npu-operator" \
    org.opencontainers.image.vendor="Rebellions Inc."

# Pick up OpenSSL errata that may not yet be included in the UBI base image.
RUN dnf upgrade --refresh -y openssl openssl-libs && dnf clean all

COPY LICENSE NOTICE THIRD_PARTY_NOTICES /licenses/
