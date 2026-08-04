# RBLN NPU Operator

The RBLN NPU Operator automates the deployment and management of all Rebellions software components required for provisioning the RBLN NPU family Kubernetes and OpenShift clusters. A singleton `RBLNClusterPolicy` custom resource orchestrates every operand, ensuring that device plugins, metrics, and VFIO-based passthrough stacks stay aligned with the hardware available in each node pool.

## Key Capabilities

- **Lifecycle automation** – Automatically manages the full NPU exposure flow, from detecting PCI `1eff:*` functions and labeling nodes to deploying the appropriate device plugins per workload type.
- **Dual workload types** – Container mode publishes resources (for example `rebellions.ai/npu`) through the standard device plugin, while sandbox mode rebinds functions to `vfio-pci` and advertises VFIO-backed resources such as `rebellions.ai/ATOM_CA25_PT`.

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
   - **Device Plugin** publishes `rebellions.ai/npu` resources.
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

## Operator Metrics

The operator serves its own metrics (distinct from the NPU device metrics exposed by the
`rbln-metrics-exporter` operand) at `https://<pod>:8443/metrics`, protected by
kube-apiserver-backed authentication and authorization. A default install exposes the
authenticated endpoint and Service; scraping additionally requires enabling the
ServiceMonitor and binding the scraper's ServiceAccount (both steps below). Disable the
endpoint entirely with `--set operator.metrics.enabled=false`.

> Migration note: the former `operator.service.{type,port,targetPort}` values were replaced
> by `operator.metrics.{enabled,port}` (`type` was never consumed by the chart). With
> `helm upgrade --reuse-values`, set the new keys explicitly or re-supply your values file.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `rbln_operator_clusterpolicy_reconcile_status` | Gauge | — | Last policy reconcile outcome (0=success, 1=notReady, 2=unavailable) |
| `rbln_operator_driver_reconcile_status` | Gauge | `name` | Last driver reconcile outcome per CR |
| `rbln_operator_reconcile_total` | Counter | `controller` | Reconciliations executed |
| `rbln_operator_reconcile_failed_total` | Counter | `controller` | Reconciliations ended non-ready |
| `rbln_operator_npu_nodes` | Gauge | `workload` | NPU-eligible nodes per workload type |
| `rbln_operator_workload_coverage_state` | Gauge | `workload` | Aggregate state (0=empty ~ 3=uncovered) |
| `rbln_operator_driver_pool_ready_ratio` | Gauge | `driver`, `pool` | Ready/desired ratio per driver pool |
| `rbln_operator_driver_upgrade_nodes` | Gauge | `state` | Nodes per driver upgrade state |

Standard controller-runtime metrics (`controller_runtime_*`, `workqueue_*`, `rest_client_*`, `go_*`)
are exposed on the same endpoint.

### Scraping with Prometheus Operator

```bash
helm upgrade rbln-npu-operator ... --set operator.metrics.serviceMonitor.enabled=true
```

The ServiceMonitor is rendered only when the `monitoring.coreos.com/v1` CRD is present
(GitOps flows using `helm template` must pass `--api-versions monitoring.coreos.com/v1`).
The scrape target reaches UP only after granting the scraper's ServiceAccount access —
this binding is a required step, not optional:

```bash
kubectl create clusterrolebinding rbln-operator-metrics-scraper \
  --clusterrole=<release>-rbln-npu-operator-metrics-reader \
  --serviceaccount=<monitoring-ns>:<scraper-sa>
```

Example queries:

```promql
sum by (state) (rbln_operator_driver_upgrade_nodes)
rate(rbln_operator_reconcile_failed_total[5m]) > 0
max(rbln_operator_workload_coverage_state) >= 3
min(rbln_operator_driver_pool_ready_ratio) < 1
```

## Operator Events

The operator records Kubernetes Events for state transitions and failures, so
`kubectl describe` shows what happened and when. Reasons are a stable contract:

| Reason | Type | Object | When | Paired `Ready` condition |
|--------|------|--------|------|--------------------------|
| `DriverUpgradeStarted` | Normal | Node | upgrade-required → cordon-required | — (node label) |
| `NodeDrained` | Normal | Node | node drain succeeded | — (node label) |
| `NodeDrainFailed` | Warning | Node | cordon or drain failed | — (node label) |
| `DriverUpgradeCompleted` | Normal | Node | in-progress state → upgrade-done | — (node label) |
| `DriverUpgradeFailed` | Warning | Node | non-Failed state → upgrade-failed | — (node label) |
| `ComponentApplyFailed` | Warning | RBLNClusterPolicy | component apply failed | `False / ComponentApplyFailed` |
| `DriverInstallFailed` | Warning | RBLNDriver | driver component apply failed | `False / Error` |
| `ConflictingNodeSelector` | Warning | RBLNDriver | another driver already claims these nodes | `False / ConflictingNodeSelector` |
| `AllActiveWorkloadsReady` | Normal | RBLNClusterPolicy | transition to ready | `True / AllActiveWorkloadsReady` |
| `DriverReady` | Normal | RBLNDriver | transition to ready | `True / AllDriverPoolsReady` |
| `PolicyIgnored` | Normal | RBLNClusterPolicy | non-singleton policy ignored | `False / PolicyIgnored` |

Steady-state reconciles do not re-emit Normal events, and the transition-scoped
Warnings (`ConflictingNodeSelector`) fire once per spec generation rather than on
every retry.

Events are **not durable**: the API server garbage-collects them (default
`kube-apiserver --event-ttl=1h`). Treat them as the recent-history view, use
`.status.conditions` for the current authoritative state, and use the
`rbln_operator_*` metrics above for long-term trends and alerting. Every
CR-scoped failure above is mirrored into a condition for exactly this reason.

If an expected event is missing, note that client-go's spam filter keys on
`(source, object, type)` and **ignores the reason** — one bucket of 25 events per
object, refilling at 1 per 5 minutes. Splitting a failure across more reasons
does not raise that budget.

Both CRDs are cluster-scoped, so their events land in the `default` namespace
(not the operator's namespace). Inspect with:

```bash
kubectl get events -A --field-selector involvedObject.kind=Node,involvedObject.name=<node> --sort-by=.lastTimestamp
kubectl describe rblnclusterpolicy <name>
kubectl describe rblndriver <name>
```

## Support & Resources

- Helm chart & source: [RBLN NPU Operator Helm chart](https://github.com/rebellions-sw/rbln-npu-operator/tree/main/deployments/rbln-npu-operator)
- Issues & feature requests: open a GitHub issue in this repository.
- Kubebuilder reference: [kubebuilder.io](https://book.kubebuilder.io)
- For cluster-specific guidance, contact your Rebellions support representative.
