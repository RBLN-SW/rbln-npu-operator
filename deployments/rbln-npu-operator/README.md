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
- Driver images published under family-scoped registry paths (BREAKING since
  this version): `{registry}/{org}/{family}/{name}:{version}-{kernel}-{os}`
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

Driver deployment is disabled by default (`driver.enabled: false`). Set
`--set driver.enabled=true` to deploy ATOM **3.4.1** and REBEL100 **3.5.1**
through the built-in family instances (`driver.deployDefault: false`). The
same configuration supports ATOM-only, REBEL100-only, and mixed clusters:
an instance with no matching nodes stays Ready with no driver or SMD
DaemonSet. NFD must supply the node labels used for routing.

Override `driver.instances.atom.image.tag` or
`driver.instances.rebel100.image.tag` to change these versions. Changing
`driver.image.tag` alone does not override the family pins. Setting
`driver.enabled: false` suppresses all driver CRs even with instances configured.

When upgrading an existing default `rbln-driver` deployment, review the
[migration guide](../../docs/rolling-driver-migration.md) before adopting
these defaults: disabling the fallback CR changes node ownership and replaces
its driver DaemonSets.

### Using Sample Configurations

This chart provides three pre-configured sample values files for different workload types:

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

#### Multi-Driver Deployments
For clusters with multiple NPU families or versions, running per-node-group driver versions:
```bash
helm install rbln-npu-operator ./rbln-npu-operator \
  -f ./rbln-npu-operator/sample-values-MultiDriver.yaml \
  --create-namespace \
  --namespace rbln-system
```
The `rebellions.ai/npu.family` labels used by the instance selectors are
applied automatically by the operator's `NodeFeatureRule` (requires NFD).
Each instance's image is pulled from that same family-scoped registry path
(BREAKING; see [Driver image layout](../../README.md#driver-image-layout-breaking)).
See the root README's [Multiple driver versions](../../README.md#multiple-driver-versions)
section for the `driver.instances` inheritance rules, routing/tie behavior, and
migration notes.

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

### Component update strategies

Each of `devicePlugin`, `draKubeletPlugin`, `sandboxDevicePlugin`,
`metricsExporter`, `npuFeatureDiscovery`, `containerToolkit`, `vfioManager`,
and `validator` accepts an optional `updateStrategy` in Helm values. The same
field is available under `spec.<component>.updateStrategy` in an
`RBLNClusterPolicy`.

```yaml
devicePlugin:
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
metricsExporter:
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: "10%"
containerToolkit:
  updateStrategy:
    type: OnDelete
```

- The default is an empty map (`updateStrategy: {}` in Helm values), which
  Helm treats as unset and does not render. An explicit `null` also leaves it unset.
  In a direct `RBLNClusterPolicy` manifest, omit the field or use
  `updateStrategy: null`; `{}` there explicitly selects RollingUpdate defaults.
  Existing DaemonSet strategies, including manual overrides, are preserved.
  New DaemonSets keep the existing Kubernetes defaults: `RollingUpdate`,
  `maxUnavailable: 1`, and `maxSurge: 0`. Adding this feature alone does not
  change pod templates or restart existing component pods.
- An explicitly configured strategy defaults to `RollingUpdate` and
  `maxUnavailable: 1`. `maxUnavailable` accepts a positive integer or a quoted
  percentage from `"1%"` to `"100%"`, independently for each DaemonSet.
  Surge updates are not exposed; configured rolling updates use `maxSurge: 0`.
- `OnDelete` requires `rollingUpdate` to be omitted. When switching using
  merged Helm values (including `--reuse-values`), set
  `<component>.updateStrategy.rollingUpdate=null` to clear any previous block.
  An image/template change then waits until the old pods are deleted before
  taking effect. `Ready` reports running pod availability, not that every pod
  uses the latest template.
- Removing the setting stops managing the strategy and preserves its current
  value. To restore automatic rolling updates, explicitly set
  `updateStrategy: {type: RollingUpdate}` and clear any previous
  `rollingUpdate` block. Pending template changes can roll out as soon as this
  switch is applied.
- Driver and SMD are excluded and retain `OnDelete`. Continue using
  `driver.upgradePolicy` for the driver upgrade workflow.

The updated CRD must be installed before using these fields; the hook below
handles this for Helm upgrades by default. With externally managed CRDs,
apply the new schema first. Old values without these keys remain valid.
Rolling back to an operator version without this feature does not reset a
strategy already applied to a DaemonSet. Restore the desired strategy before
rollback if necessary.

### Upgrades and CRDs

Helm applies the CRDs under `crds/` only on the **first** `helm install` and, by
design, never updates them on `helm upgrade`. Because the operator's CRD schema
evolves across releases, this chart keeps the CRDs in sync automatically via a
`pre-install`/`pre-upgrade` hook Job (`crds.upgrade.enabled`, default `true`):

```bash
helm upgrade rbln-npu-operator ./rbln-npu-operator -n rbln-system
```

The hook runs the operator image's `crd-apply` binary, which server-side-applies
the CRDs baked into the image at `/opt/rbln/crds`. There is no separate `kubectl`
image to pin, no ConfigMap size limit, and no manual `kubectl apply` step. Because
the operator image is non-root by default and sets no fixed UID, the hook runs on
both vanilla Kubernetes and OpenShift (whose SCC assigns an in-range UID).

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
