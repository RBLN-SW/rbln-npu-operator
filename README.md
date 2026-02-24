# RBLN NPU Operator

The RBLN NPU Operator automates the deployment and management of all Rebellions software components required for provisioning the RBLN NPU family Kubernetes and OpenShift clusters. A singleton `RBLNClusterPolicy` custom resource orchestrates every operand, ensuring that device plugins, metrics, and VFIO-based passthrough stacks stay aligned with the hardware available in each node pool.

## Key Capabilities

- **Lifecycle automation** – Automatically manages the full NPU exposure flow, from detecting PCI `1eff:*` functions and labeling nodes to deploying the appropriate device plugins per workload type.
- **Dual workload types** – Container mode publishes resources (for example `rebellions.ai/ATOM`) through the standard device plugin, while sandbox mode rebinds functions to `vfio-pci` and advertises VFIO-backed resources such as `rebellions.ai/ATOM_CA25_PT`.

## Compatibility Matrix

| Category | Minimum Version | Notes |
| --- | --- | --- |
| Kubernetes | v1.19+ | Validated through v1.32+ |
| OpenShift | 4.19+ | Detected automatically; SCC integration enabled |
| Helm | v3.9 | Required for chart install/upgrade |
| Container Runtime | containerd | Needs hostPath access to `/dev`, `/sys`, kubelet plugin dirs |

## High-Level Architecture

```text
RBLNClusterPolicy (Cluster-scoped CR)
└── Controller Manager (Singleton reconciliation loop)
    ├─ Node labeling (NFD dependency, workload labels)
    ├─ Device Plugin DaemonSet + ConfigMap
    ├─ DRA Kubelet Plugin DaemonSet
    ├─ Sandbox Device Plugin + VFIO Checker
    ├─ VFIO Manager DaemonSet
    ├─ Metrics Exporter DaemonSet
    └─ NPU Feature Discovery DaemonSet
```

## Workload Profiles

### Container (default)

1. Enable by keeping `spec.workloadType` (or the Helm value) set to `container`, which is the default.
2. Components:
   - **Device Plugin** publishes `rebellions.ai/ATOM` resources.
   - **DRA Kubelet Plugin** enables workloads to consume NPUs through Kubernetes Dynamic Resource Allocation (DRA).
   - **Metrics Exporter** exposes Prometheus-ready telemetry.
   - **NPU Feature Discovery** labels nodes with RBLN hardware inventory.
   - Leaves native RBLN drivers bound for container passthrough workloads.

### Sandbox / VM Passthrough

1. Enable via Helm values or set `spec.workloadType: vm-passthrough`
2. Components:
   - **NPU Feature Discovery** continues to label VFIO-ready nodes so sandbox DaemonSets pin only to hardware that matches the policy.
   - **VFIO Manager** rebinding script (`vfio-manage.sh`) detaches vendor devices and binds them to `vfio-pci`.
   - **Sandbox Device Plugin** advertises `rebellions.ai/ATOM_*_PT` resources.
   - **VFIO Checker** ensures nodes remain in a ready state for KubeVirt.
3. KubeVirt integration:
   - Enable `HostDevices` feature gate
   - Populate `permittedHostDevices` with vendor selector `1eff:XXXX`
   - Reference `rebellions.ai/ATOM_CA25_PT` inside `VirtualMachine.spec.template.spec.domain.devices.hostDevices`

## Quick Start

The RBLN NPU Operator Helm chart automatically discovers RBLN NPUs in your cluster, deploys the required device plugins, and monitors the health of each operand. Follow these steps to get up and running quickly.

1. **Prerequisites**
    - Kubernetes 1.19+ cluster with access to `kubectl` and `helm`
    - A dedicated namespace such as `rbln-system` is recommended
    - Worker nodes equipped with NPUs and Node Feature Discovery installed (set `nfd.enabled=true` if you want Helm to deploy it)

2. **Install Helm (if needed)**
   ```bash
   curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 \
     && chmod 700 get_helm.sh \
     && ./get_helm.sh
   ```

3. **Add the Rebellions Helm repository**
   ```bash
   helm repo add rebellions https://rebellions-sw.github.io/rbln-npu-operator
   helm repo update
   ```

4. **Install the NPU Operator**
   ```bash
   helm install --wait --generate-name \
        -n rbln-system --create-namespace \
        rebellions/rbln-npu-operator
   ```

### Verify Installation

After Helm reports a successful install, confirm that the `RBLNClusterPolicy` custom resource is present and reconciled:

```bash
kubectl get rblnclusterpolicies.rebellions.ai -n rbln-system
NAME                  AGE
rbln-cluster-policy   8m
```

Next, inspect the operator namespace to verify the health of the controller and operand pods:

```bash
kubectl get pods -n rbln-system
NAME                                             READY   STATUS    AGE
controller-manager-797798d7b8-rjzht              1/1     Running   8m
rbln-device-plugin-4qgxc                         1/1     Running   8m
rbln-metrics-exporter-jghbg                      1/1     Running   8m
rbln-npu-feature-discovery-zg47r                 1/1     Running   8m
```

## Support & Resources

- Helm chart & source: [RBLN NPU Operator Helm chart](https://github.com/rebellions-sw/rbln-npu-operator/tree/main/deployments/rbln-npu-operator)
- Issues & feature requests: open a GitHub issue in this repository.
- Kubebuilder reference: [kubebuilder.io](https://book.kubebuilder.io)
- For cluster-specific guidance, contact your Rebellions support representative.
