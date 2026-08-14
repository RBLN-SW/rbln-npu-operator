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
    ├─ NPU Family NodeFeatureRule (rebellions.ai/npu.family)
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
    - To enable the driver (`driver.enabled=true`, with or without `driver.instances`), a `drivercred` image-pull secret in the target namespace — every driver sample/values file references it by name:
      ```bash
      kubectl create secret docker-registry drivercred \
        --docker-server=repo.rebellions.ai \
        --docker-username=<user> --docker-password=<password> \
        -n rbln-system
      ```

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
| `rbln_operator_driver_owned_nodes` | Gauge | `driver` | Nodes routed to each RBLNDriver (zero-seeded, so a selector matching nothing reads 0) |
| `rbln_operator_driver_uncovered_nodes` | Gauge | — | Deploy-labeled NPU nodes with no owning RBLNDriver |
| `rbln_operator_driver_selector_conflict_nodes` | Gauge | `driver` | Nodes where this driver's selector ties with another RBLNDriver |
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
| `DriverNodeUncovered` | Warning | Node | node loses its RBLNDriver owner (no selector matches, or an unresolved selector tie); transition-only | — (node label) |
| `DriverOwnerChanged` | Normal | Node | node's RBLNDriver owner is set or changed; transition-only | — (node label) |
| `ComponentApplyFailed` | Warning | RBLNClusterPolicy | component apply failed | `False / ComponentApplyFailed` |
| `DriverInstallFailed` | Warning | RBLNDriver | driver component apply failed | `False / Error` |
| `InvalidSpec` | Warning | RBLNDriver | nodeSelector uses a reserved key, or name exceeds 63 characters | `False / InvalidSpec` |
| `ConflictingNodeSelector` | Warning | RBLNDriver | nodeSelector ties with another RBLNDriver on specific nodes | `False / ConflictingNodeSelector` |
| `DriverImageNotFound` | Warning | RBLNDriver | a driver pool's composed image is missing from its registry; re-checked periodically (≈5m), self-heals once the image is published | `False / DriverImageNotFound` |
| `DriverFamilyLabelMissing` | Warning | RBLNDriver | an owned node has no usable `rebellions.ai/npu.family` label; fix NFD/the NodeFeatureRule or the label value | `False / DriverFamilyLabelMissing` |
| `AllActiveWorkloadsReady` | Normal | RBLNClusterPolicy | transition to ready | `True / AllActiveWorkloadsReady` |
| `DriverReady` | Normal | RBLNDriver | transition to ready | `True / AllDriverPoolsReady` |
| `PolicyIgnored` | Normal | RBLNClusterPolicy | non-singleton policy ignored | `False / PolicyIgnored` |

Steady-state reconciles do not re-emit Normal events. CR-scoped Warnings
(`ConflictingNodeSelector`, `InvalidSpec`, `DriverImageNotFound`,
`DriverFamilyLabelMissing`) fire once per `(reason, spec generation)` rather
than on every retry. For `ConflictingNodeSelector`/`InvalidSpec` the spec edit
is also what triggers the condition, so the dedup key lines up with what
changed; `DriverImageNotFound`/`DriverFamilyLabelMissing` are triggered by
node labels or registry state instead, so a problem that clears and later
recurs under an unchanged spec updates the condition but does not re-notify
via event — `.status.conditions` and the `rbln_operator_*` metrics still
track every occurrence. The per-node routing events (`DriverNodeUncovered`,
`DriverOwnerChanged`) follow a different rule: they fire only on the
reconcile pass where a node's owner actually changes, never on steady state,
regardless of spec generation.

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

## Multiple driver versions

The Helm chart's `driver.instances` map runs more than one `RBLNDriver`
version in the same cluster — for example a cluster-wide default plus a
canary version for a labeled subset of nodes, or a different version per NPU
family:

```yaml
driver:
  enabled: true
  image:
    tag: "3.2.2"          # fallback for any node not claimed below
  instances:
    atom-canary:
      image:
        tag: "3.3.0-rc1"
      nodeSelector:
        rebellions.ai/npu.family: atom
        env: canary
```

Each key renders its own CR named `rbln-driver-<key>`; keys must be a
lowercase DNS-1123 label of at most 40 characters (the name is stamped onto
nodes as a label value, which caps the full CR name at 63 characters).
kubectl users applying raw CRs instead of Helm can start from
[`config/samples/v1alpha1_rblndriver_instances.yaml`](config/samples/v1alpha1_rblndriver_instances.yaml).

### Automatic family labels

The operator itself deploys a `NodeFeatureRule` named `rbln-npu-family` that
labels every NPU node with `rebellions.ai/npu.family: atom|rebel100` derived
from its PCI device ID, so per-family instances need no manual node labeling.
The rule is created in every NodeFeatureRule API the cluster serves —
upstream NFD (`nfd.k8s-sigs.io`) and the Red Hat NFD Operator
(`nfd.openshift.io`) — which makes it identical for Helm and OLM installs.
If neither API is served, the `rbln-npu-family` component reports `notReady`
in the `RBLNClusterPolicy` status until NFD is installed. The device-ID →
family mapping lives in `internal/consts` (`NPUFamilies`).

### Driver image layout (BREAKING)

Driver images are pulled from family-scoped registry paths:
`{registry}/{org}/{family}/{name}:{version}-{kernel}-{os}` — e.g.
`repo.rebellions.ai/rebellions/atom/rbln-driver:3.5.0-<kernel>-<os>`. The
family segment is the node's `rebellions.ai/npu.family` label value. Before
upgrading the operator, every driver version in use must be published under
its family path; flat-path images are no longer pulled. A single RBLNDriver
spanning several families needs the same version published for **every**
family it owns — use per-family `driver.instances` when versions diverge.

The operator verifies each composed image in its registry before rendering
the pool DaemonSet (manifest HEAD with the CR's imagePullSecrets — exempt
from Docker Hub pull-rate limits). A missing image marks the CR
`Ready=False`/`DriverImageNotFound` and is re-checked periodically (≈5
minutes), so publishing the image heals without intervention. Pool DaemonSet
names embed the family, so a flat (pre-family) DaemonSet left by an older
operator version is never updated or migrated in place — it is replaced, and
the replacement is fail-safe: the flat DaemonSet is reaped only once every
owned node carries a usable `npu.family` label **and** every family pool's
image is confirmed present in the registry. Until then the operator keeps it
running and defers creating the family pools it still covers, so an upgrade
never tears down the running driver without a replacement, and never runs
two driver installers on one host. Publishing the family-scoped images
**before** upgrading the operator remains the recommended order — the
switchover is then immediate instead of waiting on the ≈5-minute recheck.
(With the registry check disabled this image gate is skipped, so mirror the
family images first.) The switchover itself is per-pool, not per-node; to
pace the node-by-node driver reinstall, follow the taint-gated procedure in
[docs/rolling-driver-migration.md](docs/rolling-driver-migration.md).
Only a 404 fails a pool. Every other outcome — 401/403, timeout, TLS or DNS
failure, an unparseable reference, or a 404 from the `/v2/` handshake rather
than the manifest itself — logs a warning and proceeds, leaving kubelet the
final arbiter. If the operator could not read a configured
`spec.imagePullSecrets` entry the check still runs (anonymously) and a 404
still blocks, but the condition message names the unread secret instead of
telling you to publish the image: on a registry that answers 404 rather than
401/403 to unauthorized reads, a private image that already exists reads as
missing. Disable the check entirely with `operator.driverImageCheck=false` when
the operator cannot reach the registry nodes pull from (mirror-only or
disconnected clusters), or when node-level credentials (a ServiceAccount pull
secret, `/var/lib/kubelet/config.json`, ECR/GCR instance identity) authenticate
the pull but are invisible to the operator. OLM installs have no Helm values to
set — add `DRIVER_IMAGE_CHECK=false` to the Subscription's `spec.config.env`
instead.

### Inheritance

Fields left unset on an instance inherit from the top-level `driver` block:

| Field kind | Example | Merge behavior |
|---|---|---|
| Maps | `image`, `resources`, `annotations`, `manager` | Deep-merged per key; `{}` is a no-op. Clearing a *single* inherited key with `null` is **not** supported — it renders a literal `null` the API server rejects at install time. Override the whole map on the instance instead, or set the field itself to `null` to drop it entirely |
| Lists | `tolerations`, `env`, `imagePullSecrets` | Replaced wholesale, never merged; `[]` clears the inherited value |
| Scalars | `priorityClassName` | Overridden; `""` clears the inherited value |
| `nodeSelector` | — | **Never inherited.** Required and non-empty on every instance — only the default (top-level `driver`) instance may have an empty/absent selector |

### How routing works

The driver DaemonSet never selects on your `nodeSelector` directly. On every
reconcile the operator recomputes ownership for every NPU node
(`rebellions.ai/npu.deploy.driver=true`) and stamps the winning CR's name onto
node label `rebellions.ai/npu.driver.owner`; each driver's DaemonSet selects
on that owner label (together with the deploy gate and the pool's NFD
os/kernel labels), never on your selector. This guarantees exactly one driver
per node regardless of how selectors overlap.

The winner for a node is the **most specific** matching selector — one that is
a strict superset of every other matching selector. While the top-level
`driver.nodeSelector` is left empty (the default), the default instance
matches every node and automatically falls back for any node no more specific
instance claims: `{}` ⊂ `{family: atom}` ⊂ `{family: atom, env:
canary}`. Reserved keys `rebellions.ai/npu.driver.owner` and
`rebellions.ai/npu.deploy.driver` must not appear in any `nodeSelector`; a CR
that sets one is rejected with `InvalidSpec` and excluded from routing without
affecting any other CR.

### Overlaps, ties, and self-healing

Two instances with identical or otherwise incomparable selectors (neither is
a superset of the other) tie on any node both match. The tie is resolved
per-node: the tied node keeps its current owner if it has one (sticky) or
stays unassigned, while every other node routes normally. Both tied CRs
report `Ready=False` / `ConflictingNodeSelector` with a sample of the
affected nodes — reconciliation is not blocked, and editing either selector
resolves the tie on the next reconcile automatically.

`deployDefault: false` is **destructive on a live cluster**: it prunes the
default/fallback CR, so every node not claimed by a more specific instance
loses its driver. Those are exactly the nodes the default CR currently owns —
list them before flipping it (an `!npu.driver.owner` query is useless here:
while the default CR still exists, it owns these nodes and the query comes
back empty):

```bash
kubectl get nodes -l 'rebellions.ai/npu.driver.owner=rbln-driver'
```

### Adding, removing, and renaming instances

- **Add** a key: the operator claims matching nodes on the next reconcile,
  with no impact on nodes already owned by a more specific instance.
- **Remove** a key: Helm prunes the CR and the operator releases its nodes,
  which are then reassigned to whatever instance is next most specific (or
  the default, or left uncovered).
- **Rename** a key: this is a prune-and-create in the same `helm upgrade`.
  Expect a transient `ConflictingNodeSelector` (or brief uncovered window)
  while the old CR is being removed and the new one created — affected nodes
  keep their current driver throughout (sticky, never two drivers on one
  node), and the condition self-heals once the old CR is gone.

### Upgrade policy is cluster-wide

`driver.upgradePolicy` renders once onto `RBLNClusterPolicy` and applies
identically to every driver instance. Per-instance upgrade cadence — for
example, upgrading a canary instance independently of the default — is not
supported.

### Verifying after install or upgrade

```bash
kubectl get rblndrivers

# Which CR currently owns each node:
kubectl get nodes -L rebellions.ai/npu.driver.owner

# Nodes eligible for a driver but currently unowned (misconfigured selectors):
kubectl get nodes -l 'rebellions.ai/npu.deploy.driver=true,!rebellions.ai/npu.driver.owner'

# Every rendered instance and the version it will run — run this whenever you
# edit driver.instances, since a typo'd key silently inherits every field
# from the default instance, including version:
helm template rbln-npu-operator deployments/rbln-npu-operator -f my-values.yaml \
  | yq 'select(.kind == "RBLNDriver") | .metadata.name + " " + .spec.version'
```

Configure `driver.instances` with `-f my-values.yaml`, not `--set`: selector
keys contain dots (e.g. `rebellions.ai/npu.family`), and `--set` only handles
those via error-prone backslash-escaping.

### Uninstalling

`helm uninstall` removes the `RBLNDriver` CRs but does not remove the
`rebellions.ai/npu.driver.owner` labels stamped on nodes. Clean them up if
you are decommissioning the driver entirely:

```bash
kubectl label nodes --all rebellions.ai/npu.driver.owner-
```

A stale owner label is harmless on reinstall — the operator recomputes and
overwrites it on the first reconcile.

## Support & Resources

- Helm chart & source: [RBLN NPU Operator Helm chart](https://github.com/rebellions-sw/rbln-npu-operator/tree/main/deployments/rbln-npu-operator)
- Issues & feature requests: open a GitHub issue in this repository.
- Kubebuilder reference: [kubebuilder.io](https://book.kubebuilder.io)
- For cluster-specific guidance, contact your Rebellions support representative.
