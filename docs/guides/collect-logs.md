# Collect component logs

Use this guide to identify a failing component and temporarily enable debug logs.
The examples use `rbln-system`; substitute your installation namespace.

## 1. Find the Pod and container

```bash
kubectl get pods -n rbln-system -o wide
kubectl describe pod <pod-name> -n rbln-system
kubectl logs <pod-name> -n rbln-system -c <container-name> --tail=200
```

Use the container names in the Pod description. For a restarted container, add
`--previous` to read its last terminated instance. For a driver installation
failure, inspect the `k8s-driver-manager` init container first, then the driver
container if initialization completed.

## 2. Enable debug output when needed

For a Helm-managed component, merge this example into your existing values and
apply the [chart upgrade procedure](upgrade-chart.md):

```yaml
devicePlugin:
  logging:
    level: debug
    format: json
```

Use the appropriate component key:

| Component | Helm values key |
| --- | --- |
| Device Plugin | `devicePlugin` |
| DRA Driver | `draKubeletPlugin` |
| Device Config Manager | `deviceConfigManager` |
| Metrics Exporter | `metricsExporter` |
| NPU Feature Discovery | `npuFeatureDiscovery` |
| Sandbox Device Plugin | `sandboxDevicePlugin` |

For a directly managed `RBLNClusterPolicy`, put the same block under `spec` in
your source manifest. Each component image must support these logging settings.

Changing logging updates the Pod template. With `OnDelete`, the new setting takes
effect only after planned Pod replacement; see [component update strategies](configure-update-strategies.md).

## 3. Verify and restore

Read logs from the updated Pod and confirm debug records appear. Keep the
component version, node, relevant events, and the failing Pod or ResourceClaim
name with the logs so the failure can be traced.

After diagnosis, set `logging.level: info` and apply the same deployment procedure.
See [deployment checks](check-deployment.md) to verify recovery.
