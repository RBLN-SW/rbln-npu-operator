# NPU Driver Auto Upgrade

This document covers the rollout the operator performs when `spec.driver.upgradePolicy.autoUpgrade` is `true`: how a node moves from the old driver to the new one, which policy options change that path, what happens when a step fails, and where to look while a rollout is running.

This document covers:

-   **Enabling auto upgrade**: the Helm values and the policy reference
-   **Upgrade flow**: the per-node state machine and where each option applies
-   **Skipped and failed nodes**: how a failed step is classified and how to retry it
-   **Monitoring**: the `RBLNClusterPolicy` status, conditions, node annotations, and events

------------------------------------------------------------------------

## Enabling Auto Upgrade

The driver DaemonSet uses the `OnDelete` update strategy, so a new driver image changes nothing on a node until its driver pod is deleted. With `autoUpgrade: true` the operator deletes it: the node is cordoned, NPU workloads are moved off, the driver pod is replaced, the node is optionally rebooted, validated, and uncordoned. `maxParallelUpgrades` nodes go through this at a time.

A node enters a rollout when the revision of its driver pod differs from the current revision of the driver DaemonSet, or when it carries the annotation `rebellions.ai/npu-driver-upgrade-requested=true`.

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
    drain:
      enable: false
      force: false
      deleteEmptyDirData: false
      podSelector: ""
      timeoutSeconds: 300
    reboot:
      enable: false
      rebootTimeoutSeconds: 0
```

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
| `drain.enable` | `true` = drain the whole node after NPU pod eviction | `false` |
| `drain.force` | `true` = proceed even when pods block the drain | `false` |
| `drain.deleteEmptyDirData` | `true` = also remove pods that use `emptyDir` storage | `false` |
| `drain.podSelector` | Label selector restricting the drain. Empty = all pods | `""` |
| `drain.timeoutSeconds` | Maximum seconds for the drain. `0` = wait indefinitely | `300` |
| `reboot.enable` | `true` = reboot the node after the driver pod is replaced | `false` |
| `reboot.rebootTimeoutSeconds` | Maximum seconds for the reboot and the post-reboot stabilization. `0` = no reboot timeout; stabilization then uses 600 | `0` |

> [!WARNING]
> **Enable drain together with reboot.** `reboot.enable` does not imply `drain.enable`. With `reboot.enable: true` and `drain.enable: false`, only pods that request an NPU are evicted, and the node reboots with every other pod still running on it, without eviction or PodDisruptionBudget checks.

------------------------------------------------------------------------

## Upgrade Flow

Each node advances through the states below one step at a time. The current state is the value of the node label `rebellions.ai/npu-driver-upgrade-state`.

| State | Action | Result on failure |
|:-----:|--------|:-----------------:|
| `upgrade-required` | Waits for a parallelism slot | N/A |
| `cordon-required` | Cordons the node | retried |
| `wait-for-jobs-required` | Waits until no pod matching `waitForCompletion.podSelector` is Running or Pending. Skipped when the selector is empty | proceeds on timeout |
| `pod-deletion-required` | Evicts pods that request a `rebellions.ai/*` resource, then moves to `drain-required` when `reboot.enable` is set and to `pod-restart-required` otherwise | `upgrade-skipped`, or `drain-required` when `drain.enable` is set |
| `drain-required` | Drains the node when `drain.enable` is set; otherwise passes through | `upgrade-skipped` |
| `pod-restart-required` | Deletes the driver pod and waits for the replacement to become Ready | `upgrade-failed` |
| `reboot-required`, `reboot-validation-required`, `reboot-post-required` | When `reboot.enable` is set: triggers the reboot, waits for the boot ID to change, then waits for the node and its DaemonSet pods to become Ready | `upgrade-failed` |
| `validation-required` | Waits up to 600 seconds for the operator validator pod on the node to become Ready | `upgrade-failed` |
| `uncordon-required` | Uncordons the node. A node that was already cordoned before the upgrade stays cordoned | retried |
| `upgrade-done` | Terminal until the next driver revision | N/A |

`maxParallelUpgrades` counts every node between `cordon-required` and `uncordon-required`, plus every node in `upgrade-failed`. Nodes in `upgrade-skipped` and `upgrade-done` do not count.

------------------------------------------------------------------------

## Skipped and Failed Nodes

A failed step parks the node in one of two states, chosen by where in the flow the failure happened.

| Aspect | `upgrade-skipped` | `upgrade-failed` |
|--------|-------------------|------------------|
| Failed step | Eviction or drain, before the driver is touched | Pod restart, reboot, or validation |
| Node | Uncordoned, back in service on the old driver | Stays cordoned |
| Parallelism slot | Released | Held until the node leaves the state |
| Reason | Annotation `rebellions.ai/npu-driver-upgrade-skip-reason` | Annotations `rebellions.ai/npu-driver-upgrade-failure-reason` and `rebellions.ai/npu-driver-upgrade-failure-step` |
| Event | `DriverUpgradeSkipped` (Warning) | `DriverUpgradeFailed` (Warning) |

There is no limit on skipped nodes; a rollout that ends with skipped nodes reports `PartiallyComplete`. Failed nodes reduce the effective parallelism by one each, and once `maxParallelUpgrades` of them accumulate no further node is admitted until one is resolved.

During `pod-restart-required`, a driver pod in `ImagePullBackOff`, `ErrImagePull`, `CrashLoopBackOff`, or a similar waiting state raises a `DriverUpgradePodStuck` Warning event immediately. The node is marked `upgrade-failed` only when `podRestartTimeoutSeconds` elapses or the pod restarts ten times, so a transient registry outage recovers on its own.

### Retry Paths

-   **Annotation.** Fix the cause first (the PodDisruptionBudget, the stuck job, the node), then request one new attempt. The operator removes the annotation when it re-admits the node. There is no automatic retry within the same driver revision.

    ```bash
    $ kubectl annotate node <NODE_NAME> rebellions.ai/npu-driver-upgrade-requested=true
    ```

-   **New driver revision.** Publishing a new driver image starts a new rollout, and every skipped or failed node is retried once.
-   **Self-heal.** A node that failed in `pod-restart-required` resumes on its own once its driver pod becomes Ready. Reboot and validation failures never self-heal.

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
| `npu-driver-upgrade-skip-reason` | On the transition to `upgrade-skipped`: the eviction or drain error, truncated to 400 characters | When the node is retried |
| `npu-driver-upgrade-requested` | `true`, by you, to request one attempt for a done, skipped, or failed node | By the operator when it re-admits the node |

### Events

Upgrade events are recorded on the node with a stable `reason`. Where a reason annotation exists, the event message carries it.

| Reason | Type | When |
|--------|:----:|------|
| `DriverUpgradeStarted` | Normal | `upgrade-required` → `cordon-required` |
| `NodeDrained` | Normal | Drain succeeded |
| `NodeDrainFailed` | Warning | Cordon or drain failed |
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
