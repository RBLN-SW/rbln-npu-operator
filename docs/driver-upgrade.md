# NPU Driver Auto Upgrade

This document covers the rollout the operator performs when `spec.driver.upgradePolicy.autoUpgrade` is `true`: how a node moves from the old driver to the new one, which policy options change that path, what happens when a step fails, and where to look while a rollout is running.

This document covers:

-   **Enabling auto upgrade**: the Helm values and the policy reference
-   **Upgrade flow**: the per-node state machine and where each option applies
-   **Skipped and failed nodes**: how a failed step is classified and how to retry it
-   **Monitoring**: the `RBLNClusterPolicy` status, conditions, node annotations, and events

------------------------------------------------------------------------

## Enabling Auto Upgrade

The driver DaemonSet uses the `OnDelete` update strategy, so a new driver image changes nothing on a node until its driver pod is deleted. With `autoUpgrade: true` the operator deletes it: the node is cordoned, NPU workloads are moved off, the driver pod is replaced, validated, and uncordoned. `maxParallelUpgrades` nodes go through this at a time.

A node enters a rollout when the driver container configuration its driver pod was rendered with differs from the DaemonSet's current one (the `DRIVER_CONFIG_DIGEST` the operator stamps into the pod), or when it carries the annotation `rebellions.ai/npu-driver-upgrade-requested=true`. Changes that leave the driver container's spec alone, such as a new `k8s-driver-manager` image, an init container environment variable or a toleration, update the DaemonSet but start no rollout. Each node picks them up the next time its driver pod is recreated: at the next driver rollout, or on request through the annotation above, which recreates the pod whenever any part of its template is out of date.

Setting `autoUpgrade` back to `false` removes the state label from every node and clears `status.driverUpgrade` and its conditions.

```yaml
driver:
  upgradePolicy:
    autoUpgrade: true
    maxParallelUpgrades: 1
    podRestartTimeoutSeconds: 1800
    waitForCompletion:
      podSelector: ""
      timeoutSeconds: 0
    npuPodDeletion:
      force: false
      timeoutSeconds: 300
      deleteEmptyDirData: false
```

The keys above are chart values. In a `RBLNClusterPolicy` manifest the same block is `spec.driver.upgradePolicy`, where `npuPodDeletion` is named `podDeletion` — the name the operator uses when it names a knob in a skip reason.

### Upgrade Policy Reference

| Setting | Description | Default |
|---------|-------------|:-------:|
| `autoUpgrade` | `true` = the operator performs the rollout described on this page | `false` |
| `maxParallelUpgrades` | Nodes upgraded concurrently. `0` = no limit | `1` |
| `podRestartTimeoutSeconds` | Time budget for the driver pod replacement. On expiry the node is marked `upgrade-failed`. `0` = no timeout | `1800` |
| `waitForCompletion.podSelector` | Label selector of pods to wait for before the node is touched. Empty = skip the wait | `""` |
| `waitForCompletion.timeoutSeconds` | Maximum wait; on expiry the upgrade proceeds. `0` = wait indefinitely | `0` |
| `npuPodDeletion.force` | `true` = also evict pods that have no controller | `false` |
| `npuPodDeletion.timeoutSeconds` | Maximum seconds for NPU pod eviction. `0` = wait indefinitely | `300` |
| `npuPodDeletion.deleteEmptyDirData` | `true` = also evict NPU pods that mount `emptyDir` volumes; their contents are lost. `false` = such a pod parks the node in `upgrade-skipped`, named in the skip reason | `false` |

> [!NOTE]
> `npuPodDeletion.force` and `npuPodDeletion.deleteEmptyDirData` apply even with `autoUpgrade: false`. With the workflow off, `k8s-driver-manager` empties the node itself whenever a driver pod restarts while the module is still loaded, and it obeys the same two settings. `timeoutSeconds` is the exception: it bounds only the operator's eviction, which has `upgrade-skipped` to fall back to. `k8s-driver-manager` has no such state and waits instead of giving up.
>
> This needs a `k8s-driver-manager` that binds the `NPU_POD_EVICTION_*` variables, which is what the chart's `driver.manager.image.tag` pins. Releases up to v0.2.2 ignore them and, with `ENABLE_AUTO_DRAIN=false` no longer rendered, fall back to draining the whole node.
>
> A node stuck this way shows up as a driver pod whose `k8s-driver-manager` init container is in `CrashLoopBackOff`, repeating `cannot proceed until all NPU pods are evicted from the node`. Its logs name the blocking pod.

------------------------------------------------------------------------

## Upgrade Flow

Each node advances through the states below one step at a time. The current state is the value of the node label `rebellions.ai/npu-driver-upgrade-state`.

| State | Action | Result on failure |
|:-----:|--------|:-----------------:|
| `upgrade-required` | Waits for a parallelism slot | N/A |
| `cordon-required` | Cordons the node | retried |
| `wait-for-jobs-required` | Waits until no pod matching `waitForCompletion.podSelector` is Running or Pending. Skipped when the selector is empty | proceeds on timeout |
| `pod-deletion-required` | Evicts the node's NPU pods, then moves to `pod-restart-required`. A pod the eviction cannot remove parks the node; see [Why a node is skipped](#why-a-node-is-skipped) | `upgrade-skipped` |
| `pod-restart-required` | Deletes the driver pod and waits for the replacement to become Ready | `upgrade-failed` |
| `validation-required` | Waits up to 600 seconds for the operator validator pod on the node to become Ready | `upgrade-failed` |
| `uncordon-required` | Uncordons the node. A node that was already cordoned before the upgrade stays cordoned | retried |
| `upgrade-done` | Terminal until the next driver revision | N/A |

`maxParallelUpgrades` counts every node between `cordon-required` and `uncordon-required`, plus every node in `upgrade-failed`. Nodes in `upgrade-skipped` and `upgrade-done` do not count.

------------------------------------------------------------------------

## Skipped and Failed Nodes

A failed step parks the node in one of two states, chosen by where in the flow the failure happened.

| Aspect | `upgrade-skipped` | `upgrade-failed` |
|--------|-------------------|------------------|
| Failed step | Eviction, before the driver is touched | Pod restart or validation |
| Node | Uncordoned, back in service on the old driver | Stays cordoned |
| Parallelism slot | Released | Held until the node leaves the state |
| Reason | Annotation `rebellions.ai/npu-driver-upgrade-skip-reason` | Annotations `rebellions.ai/npu-driver-upgrade-failure-reason` and `rebellions.ai/npu-driver-upgrade-failure-step` |
| Event | `DriverUpgradeSkipped` (Warning) | `DriverUpgradeFailed` (Warning) |

There is no limit on skipped nodes; a rollout that ends with skipped nodes reports `PartiallyComplete`. Failed nodes reduce the effective parallelism by one each, and once `maxParallelUpgrades` of them accumulate no further node is admitted until one is resolved.

### Why a node is skipped

Only NPU pods are ever evicted; every other pod on the node is left alone. A pod counts as an NPU pod when it requests a `rebellions.ai/*` resource, or when it holds a DRA `ResourceClaim` against a DeviceClass bridged to one — the class the DRA kubelet plugin registers for container mode. A `ResourceClaim` against the passthrough DeviceClass is not evicted: it carries no such bridge, and the device it holds is bound to `vfio-pci` rather than to the driver being replaced. The skip reason names the NPU pods that blocked the eviction and how to clear them.

| Blocking NPU pod | Skip reason says | Remedy |
|------------------|------------------|--------|
| Mounts an `emptyDir` volume (for example `/dev/shm` for an inference server) | `use emptyDir volumes` | Set `npuPodDeletion.deleteEmptyDirData: true` to evict such pods (their `emptyDir` contents are lost), or move the workload and retry |
| Declares no controller | `declare no controller` | Set `npuPodDeletion.force: true`, or delete the pod and retry |
| Managed by a DaemonSet | `DaemonSet-managed` | Exclude the node from that DaemonSet and retry; an evicted DaemonSet pod is recreated on the node immediately, so no setting evicts it |
| Protected by a PodDisruptionBudget | `pod eviction failed: ... global timeout reached`, naming the pod | Adjust the budget or scale the workload, then retry |

A budget-blocked eviction is the one case the reason cannot explain: the eviction API is retried until `npuPodDeletion.timeoutSeconds` elapses, and only the timeout reaches the annotation. The rejection that names the budget goes to the operator's standard error stream, not to the structured log.

Each clause names at most three pods and summarizes the rest as `and N more`. One clause and its remedy fit the 400-character limit on the skip-reason annotation; when several kinds block the same node at once the tail is still truncated, and the operator log holds the full list.

During `pod-restart-required`, a driver pod in `ImagePullBackOff`, `ErrImagePull`, `CrashLoopBackOff`, or a similar waiting state raises a `DriverUpgradePodStuck` Warning event immediately. The node is marked `upgrade-failed` only when `podRestartTimeoutSeconds` elapses or the pod restarts ten times, so a transient registry outage recovers on its own.

### Retry Paths

-   **Annotation.** Fix the cause first (the PodDisruptionBudget, the stuck job, the node), then request one new attempt. The operator removes the annotation when it re-admits the node. There is no automatic retry within the same driver revision.

    ```bash
    $ kubectl annotate node <NODE_NAME> rebellions.ai/npu-driver-upgrade-requested=true
    ```

-   **New driver revision.** Publishing a new driver image starts a new rollout, and every skipped or failed node is retried once.
-   **Self-heal.** A node that failed in `pod-restart-required` resumes on its own once a driver pod rendered from the DaemonSet's current template becomes Ready. Validation failures never self-heal.
-   **Pod template change.** A node that failed in `pod-restart-required` is also retried once when the driver DaemonSet's pod template changes without a new driver revision, which is how a bad `k8s-driver-manager` image is fixed: the stuck pod can never become Ready from its old spec, so correcting the image re-admits the nodes it broke. Nodes in `upgrade-done` are not touched by such a change.

To exclude a node from rollouts, label it `rebellions.ai/npu-driver-upgrade.skip=true`; the node parks in `upgrade-required` until the label is removed.

------------------------------------------------------------------------

## Monitoring

### Rollout Status

The `RBLNClusterPolicy` printcolumns summarize the rollout. Skipped nodes are never counted as done.

```console
$ kubectl get rblnclusterpolicy
NAME             STATUS   ...   UPGRADE             UPGRADE-STATE
cluster-policy   ready    ...   12/40 (2 skipped)   Degraded
```

Both columns come from `status.driverUpgrade`, which the operator republishes on every reconcile:

```yaml
status:
  driverUpgrade:
    total: 40
    done: 12
    inProgress: 4
    pending: 21
    skipped: 2
    failed: 1
    state: Degraded
    progress: "12/40 (2 skipped)"
    lastTransitionTime: "2026-09-09T02:14:09Z"
```

| Field | Meaning |
|-------|---------|
| `total` | Nodes managed by the upgrade flow |
| `done` | Nodes in `upgrade-done` |
| `inProgress` | Nodes between `cordon-required` and `uncordon-required` |
| `pending` | Nodes in `upgrade-required`, plus nodes that have no state label yet |
| `skipped` | Nodes in `upgrade-skipped`. Never folded into `done` |
| `failed` | Nodes in `upgrade-failed` |
| `state` | Rollout-level disposition, derived from the counts as shown below |
| `progress` | `<done>/<total>`, with the skipped count appended when it is not zero |
| `lastTransitionTime` | Last time any count changed. Drives `UpgradeStalled` |

`state` is derived from the counts, first match wins:

| State | Condition | Meaning |
|:-----:|-----------|---------|
| `Degraded` | `failed > 0` | Failed nodes exist and each holds a parallelism slot |
| `InProgress` | `inProgress + pending > 0` | Nodes are moving or waiting for a slot |
| `PartiallyComplete` | `skipped > 0` | The rollout finished, but some nodes still run the old driver |
| `Complete` | otherwise | Every node runs the target driver |

### Conditions

| Type | True when | Reason | Message |
|------|-----------|--------|---------|
| `DriverUpgradeInProgress` | `inProgress + pending > 0` | `UpgradeInProgress`. When False: `NoUpgradeInProgress` | Node counts per disposition |
| `UpgradeDegraded` | `failed > 0`, immediately, even mid-rollout | `FailedNodes`. When False: `NoFailedNodes` | Failed nodes (up to five) with their recorded reasons, the parallelism cost, and both retry paths |
| `UpgradeIncomplete` | `skipped > 0` and `inProgress = pending = 0` | `NodesSkipped`. When False: `RolloutInProgress` while nodes are still moving, otherwise `NoSkippedNodes` | Skipped nodes (up to five) with their reasons and the retry command. With `RolloutInProgress` the message carries the running skipped count |
| `UpgradeStalled` | `inProgress > 0` and the counts have not changed for 30 minutes | `NoRecentTransition`. When False: `Progressing` | In-progress count and the time of the last transition |

`UpgradeIncomplete` waits for the rollout to settle because that is the moment a partial completion could pass for a finished one. `UpgradeStalled` requires in-flight nodes; a rollout blocked only by failed nodes holding every slot is reported by `UpgradeDegraded`, not by `UpgradeStalled`.

### Node State and Annotations

The state label is the query key, and `kubectl describe` shows the annotations that carry the reasons.

```bash
$ kubectl get nodes -L rebellions.ai/npu-driver-upgrade-state
$ kubectl get nodes -l rebellions.ai/npu-driver-upgrade-state=upgrade-failed
$ kubectl describe node <NODE_NAME>
```

All keys are prefixed `rebellions.ai/`. The operator writes the first three; `npu-driver-upgrade-requested` is the one you set.

| Annotation | Set | Cleared |
|------------|-----|---------|
| `npu-driver-upgrade-failure-reason` | On the transition to `upgrade-failed`: the failure message, truncated to 400 characters | When the node is retried or self-heals |
| `npu-driver-upgrade-failure-step` | On the transition to `upgrade-failed`: the state the node failed in | When the node is retried or self-heals |
| `npu-driver-upgrade-skip-reason` | On the transition to `upgrade-skipped`: the eviction error, truncated to 400 characters | When the node is retried |
| `npu-driver-upgrade-requested` | `true`, by you, to request one attempt for a done, skipped, or failed node | By the operator when it re-admits the node |

### Events

Upgrade events are recorded on the node with a stable `reason`. Where a reason annotation exists, the event message carries it.

| Reason | Type | When |
|--------|:----:|------|
| `DriverUpgradeStarted` | Normal | `upgrade-required` → `cordon-required` |
| `DriverUpgradeSkipped` | Warning | Node moved to `upgrade-skipped`; the message carries the skip reason |
| `DriverUpgradeFailed` | Warning | Node moved to `upgrade-failed`; the message carries the failure reason |
| `DriverUpgradePodStuck` | Warning | Driver pod replacement is not progressing; repeats every reconcile while stuck |
| `DriverUpgradeCompleted` | Normal | Node moved from an in-progress state to `upgrade-done` |

```bash
$ kubectl get events -A \
  --field-selector involvedObject.kind=Node,involvedObject.name=<NODE_NAME> \
  --sort-by=.lastTimestamp
```

### Metrics

The gauge `rbln_operator_driver_upgrade_nodes{state=...}` reports the number of nodes per state label value; nodes without a label report as `unknown`. Every state is published on every reconcile, zeros included, so an alert on `rbln_operator_driver_upgrade_nodes{state="upgrade-skipped"} > 0` or `{state="upgrade-failed"} > 0` needs no special case for a missing series.

------------------------------------------------------------------------

## Upgrading the Operator from v0.5.x or Earlier

Releases up to v0.5.0 accepted `upgradePolicy.drain` and `upgradePolicy.reboot`. Both blocks are gone: the operator evicts only the node's NPU pods and never reboots a node.

-   **Manifests.** Remove `drain` and `reboot` from any `RBLNClusterPolicy` manifest you apply directly; once the new CRD is installed, `kubectl apply` rejects them as unknown fields. If you relied on `drain.deleteEmptyDirData`, set `podDeletion.deleteEmptyDirData` instead. Helm-managed policies no longer render either block, and a `drain` block left in your values file fails the install with the same instruction (chart key: `npuPodDeletion.deleteEmptyDirData`); a leftover `reboot` block is ignored silently.
-   **Nodes mid-rollout.** Before upgrading the operator, finish or pause the rollout (`autoUpgrade: false`) so that no node is in `drain-required`, `reboot-required`, `reboot-validation-required` or `reboot-post-required`. Uncordon those nodes by hand, and delete any leftover `rbln-reboot-*` pod in the operator namespace before it reboots the node. A node left in one of the removed states past the upgrade is re-evaluated by the new operator and completes as `upgrade-done` (through the new flow when its driver pod is still the old revision, directly otherwise), but its cordon is not lifted and no event points at it.

    ```bash
    $ kubectl get nodes -l 'rebellions.ai/npu-driver-upgrade-state in (drain-required,reboot-required,reboot-validation-required,reboot-post-required)'
    $ kubectl uncordon <NODE_NAME>
    $ kubectl -n <OPERATOR_NAMESPACE> delete pod -l app.kubernetes.io/name=rbln-node-reboot
    ```

    The annotations `rebellions.ai/npu-driver-upgrade-pre-reboot-boot-id`, `-reboot-requested-at`, `-reboot-pod-name` and `-reboot-post-start-time` are left on such nodes and can be removed.
-   **Parked nodes are retried once.** This version identifies a driver revision by `DRIVER_CONFIG_DIGEST` instead of the DaemonSet controller revision. A node parked in `upgrade-skipped` or `upgrade-failed` by the old operator recorded the old identifier as its attempted revision, so on the first reconcile it reads as facing a new revision and is re-admitted once, exactly as it would be for a new driver image.
-   **Nodes in `pod-restart-required` restart their driver pod once more.** The new operator stamps every driver DaemonSet's pod template with `rebellions.ai/last-applied-template-hash` on its first pass. A driver pod the old operator already replaced carries no such stamp, so a node still in `pod-restart-required` reads its pod as outdated and recreates it one more time before completing. Nodes in `upgrade-done` are not affected.
