# RBLN NPU Operator Helm Chart

A Helm chart for deploying the Rebellions NPU Operator on Kubernetes clusters. This operator provides automated management and monitoring of Rebellions NPU devices, supporting both containerized workloads and VM passthrough scenarios.

## Overview

The RBLN NPU Operator is designed to:
- Automatically discover and manage Rebellions NPU devices in Kubernetes clusters
- Provide device plugins for containerized AI workloads
- Support VM passthrough for virtualized environments
- Export metrics for monitoring and observability
- Integrate with Node Feature Discovery (NFD) for hardware detection

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- Node Feature Discovery (NFD) - can be installed via this chart
- Rebellions NPU hardware (RBLN-CA12, RBLN-CA22, RBLN-CA25)
- For VM passthrough workloads: KubeVirt 0.50+ installed and configured

## Installation

### Quick Start

1. **Add the Helm repository** (if using a remote repository):
   ```bash
   helm repo add rebellions https://rebellions-sw.github.io/rbln-npu-operator
   helm repo update
   ```

2. **Install with default values**:
   ```bash
   helm install rbln-npu-operator ./rbln-npu-operator
   ```

### Using Sample Configurations

This chart provides two pre-configured sample values files for different workload types:

#### Container Workloads
For traditional containerized AI workloads:
```bash
helm install rbln-npu-operator ./rbln-npu-operator \
  -f ./rbln-npu-operator/sample-values-ContainerWorkload.yaml \
  --create-namespace \
  --namespace rbln-system
```

#### VM Passthrough Workloads
For virtualized environments with NPU passthrough:
```bash
helm install rbln-npu-operator ./rbln-npu-operator \
  -f ./rbln-npu-operator/sample-values-SandboxWorkload.yaml \
  --create-namespace \
  --namespace rbln-system
```

### Custom Installation

1. **Create a custom values file**:
   ```bash
   cp values.yaml my-values.yaml
   # Edit my-values.yaml with your specific configuration
   ```

2. **Install with custom values**:
   ```bash
   helm install rbln-npu-operator ./rbln-npu-operator -f my-values.yaml
   ```

### Upgrades and CRDs

Helm applies the CRDs under `crds/` only on the **first** `helm install` and, by
design, never updates them on `helm upgrade`. Because the operator's CRD schema
evolves across releases, this chart keeps the CRDs in sync automatically via a
`pre-install`/`pre-upgrade` hook Job (`crds.upgrade.enabled`, default `true`) that
re-applies `crds/` with server-side apply:

```bash
helm upgrade rbln-npu-operator ./rbln-npu-operator -n rbln-system
```

The hook uses a `kubectl` image (`crds.upgrade.image`, default
`registry.k8s.io/kubectl`); pin the tag to match your cluster's API server. No
manual `kubectl apply` of the CRDs is needed, and existing releases upgrade with
no one-time migration step.

If you deploy via a GitOps tool (Argo CD / Flux) that already reconciles the
`crds/` directory, you can disable the hook:

```yaml
crds:
  upgrade:
    enabled: false
```

CRDs are **preserved** on `helm uninstall` (never deleted), so custom resources
are not lost.

## Configuration

### Workload Types

The operator supports two main workload configurations:

#### Container Workloads (`container`)
- **Components**: Device Plugin, Metrics Exporter, NPU Feature Discovery
- **Use Case**: Traditional containerized AI applications
- **Resource Types**: `rebellions.ai/npu`

#### VM Passthrough Workloads (`vm-passthrough`)
- **Components**: Sandbox Device Plugin, VFIO Manager
- **Use Case**: Virtualized environments with NPU passthrough using KubeVirt
- **Resource Types**: One per (model, function) auto-discovered from `sysfs` + `pci.ids` — e.g. `rebellions.ai/RBLN-CA22_PF`, `rebellions.ai/RBLN-CA25_VF`
- **Requirements**: KubeVirt must be installed and VFIO-PCI driver configuration required

### KubeVirt Integration for VM Passthrough

The VM passthrough mode is specifically designed to work with KubeVirt, providing NPU device passthrough to virtual machines running on Kubernetes. This enables running AI workloads inside VMs while maintaining direct access to NPU hardware.

#### Prerequisites for VM Passthrough:
1. **KubeVirt Installation**: Ensure KubeVirt operator is installed and configured
2. **IOMMU Support**: Enable IOMMU on the host system (Intel VT-d or AMD-Vi)
3. **VFIO Modules**: Load required VFIO kernel modules (`vfio-pci`, `vfio_iommu_type1`)

## Usage Examples

### Basic Container Workload Deployment

```yaml
# values.yaml
devicePlugin:
  enabled: true
```

The plugin advertises `rebellions.ai/npu` (generic) and one product-specific
resource per detected NPU model. Set `devicePlugin.useGenericResourceName: false`
to disable the generic alias.

### VM Passthrough Deployment

```yaml
# values.yaml
sandboxDevicePlugin:
  enabled: true
vfioManager:
  enabled: true
```

The sandbox device plugin runs in per-model mode: each `(model, function)`
discovered via `sysfs` + `pci.ids` becomes its own kubelet resource (e.g.
`rebellions.ai/RBLN-CA22_PF`). KubeVirt `permittedHostDevices` must whitelist
each resource name actually consumed by VMs.

### Using NPU Resources in Pods

#### Container Workloads
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: npu-workload
spec:
  containers:
  - name: app
    image: your-ai-app:latest
    resources:
      requests:
        rebellions.ai/npu: 1
      limits:
        rebellions.ai/npu: 1
```

#### VM Passthrough Workloads
```yaml
---
apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: vm-npu-workload
spec:
  runStrategy: Always
  template:
    metadata:
      labels:
        kubevirt.io/size: large
    spec:
      domain:
        devices:
          ...
          hostDevices:
            - name: rbln0
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln1
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln2
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln3
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln4
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln5
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln6
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
            - name: rbln7
              deviceName: rebellions.ai/RBLN-CA22_PF
              tag: "pci"
        resources:
          requests:
            rebellions.ai/RBLN-CA22_PF: 8
            cpu: "4"
            memory: 50Gi
          limits:
            rebellions.ai/RBLN-CA22_PF: 8
            cpu: "4"
            memory: 50Gi
```
