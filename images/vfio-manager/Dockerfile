FROM redhat/ubi9:9.6

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

COPY LICENSE /licenses/LICENSE
