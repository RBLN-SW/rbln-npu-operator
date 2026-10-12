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
    com.redhat.component="rbln-vfio-manager"

# Pick up OpenSSL errata that may not yet be included in the UBI base image.
RUN dnf upgrade --refresh -y openssl openssl-libs && dnf clean all

COPY LICENSE /licenses/LICENSE
