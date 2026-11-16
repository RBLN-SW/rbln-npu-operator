# Configure component updates

Control how an operator-managed DaemonSet replaces Pods after an image or
configuration change. Driver and SMD updates use the separate
[driver upgrade policy](https://docs.rbln.ai/latest/software/system_management/kubernetes/upgrade_driver/index.html).

## 1. Choose a strategy

Merge the desired component setting into your installation values:

```yaml
devicePlugin:
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
```

`maxUnavailable` accepts a positive integer or a quoted percentage such as `"10%"`.
Configured rolling updates use `maxSurge: 0`. Each DaemonSet has its own limit.

To replace Pods manually after a template change:

```yaml
devicePlugin:
  updateStrategy:
    type: OnDelete
    rollingUpdate: null
```

The `null` clears a previous rolling-update block when Helm merges values.
Plan Pod replacement around workloads using the component. With `OnDelete`,
Ready Pods may still be running the previous image or configuration.

These settings apply to `devicePlugin`, `draKubeletPlugin`, `sandboxDevicePlugin`,
`metricsExporter`, `npuFeatureDiscovery`, `containerToolkit`, `deviceConfigManager`,
and `validator`. For a direct `RBLNClusterPolicy`, use `spec.<component>.updateStrategy`.

## 2. Apply and verify

Follow [Upgrade the Helm chart](upgrade-chart.md), which also explains CRD updates.
The CRD must accept `updateStrategy` before the policy uses it.

```bash
kubectl get daemonsets -n rbln-system
kubectl get daemonset <daemonset-name> -n rbln-system -o yaml
kubectl get pods -n rbln-system -o wide
```

Check the DaemonSet's strategy and updated/ready counts, then confirm the Pods use
the intended image and configuration. For `RollingUpdate`, also use
`kubectl rollout status daemonset/<daemonset-name> -n rbln-system --timeout=5m`.

## Defaults and reverting a change

- An omitted, `null`, or empty Helm value leaves the strategy unmanaged. Existing
  strategies are preserved; new DaemonSets use Kubernetes rolling-update defaults.
- In a direct policy, omit the field or use `null` to leave it unmanaged. An explicit
  `{}` instead selects the configured RollingUpdate defaults.
- Removing a setting preserves the current strategy. To resume automatic updates,
  explicitly set `type: RollingUpdate` and `rollingUpdate: null`; the configured
  default is `maxUnavailable: 1`. Pending template changes can then roll out.
- Rolling back to an operator without this setting does not undo a strategy already
  applied. Restore the desired strategy before rollback when necessary.

[Back to chart configuration](../../deployments/rbln-npu-operator/README.md)
