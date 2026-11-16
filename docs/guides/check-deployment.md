# Check the NPU deployment

Find where an installation is blocked, then confirm that the selected allocation
method can provide NPUs to workloads.

## Before you start

Complete the [installation guide](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html).
The commands below use `rbln-system`; replace it with your installation namespace.

## 1. Check the operator and components

```bash
kubectl get rblnclusterpolicies.rebellions.ai
kubectl get rblndrivers.rebellions.ai
kubectl get daemonsets,pods -n rbln-system -o wide
```

Expect the operator to be Ready and the enabled components to run on their target
nodes. The component list depends on your workload mode and enabled features.

If a resource is not ready or an expected Pod is missing, read the Conditions
and events on the policy and relevant driver:

```bash
kubectl describe rblnclusterpolicy <policy-name>
kubectl describe rblndriver <driver-name>
```

For example, `DriverImageNotFound` can prevent the driver DaemonSet from being
created, so there may be no Pod to inspect. Use the condition's reason and message
to identify the missing image, family label, or conflicting driver selector.

If a Pod exists but is Pending or waiting in an init container, inspect its events:

```bash
kubectl describe pod <pod-name> -n rbln-system
```

Check image pull and scheduling errors there. For an init container waiting on
driver readiness, [read its logs](collect-logs.md).

| Symptom | Next check |
| --- | --- |
| Driver Pod is missing or blocked in initialization | Read the driver Conditions above, then check the [driver installation settings](https://docs.rbln.ai/latest/software/system_management/kubernetes/driver_installation.html) |
| Node partition or workload-mode change is blocked | Read `NPUPartitionReady` with `kubectl describe node <npu-node>` and [collect Device Config Manager logs](collect-logs.md); for partitions, see [configuration and verification](configure-partitions.md) |
| NPU node labels are missing | Check NFD and the [label sources](https://docs.rbln.ai/latest/software/system_management/kubernetes/npu_feature_discovery.html); `npu.family` comes from the operator's `rbln-npu-family` rule |
| A workload cannot obtain an NPU | Check availability below and the [Device Plugin](https://docs.rbln.ai/latest/software/system_management/kubernetes/device_plugin.html) or [DRA](https://docs.rbln.ai/latest/software/system_management/kubernetes/npu_dra_driver.html) configuration |

For more detail from a failing component, [collect its logs](collect-logs.md).
Use documentation matching your deployment as described in
[Choose a release](choose-release.md).

## 2. Check NPU availability

For the Device Plugin or Sandbox Device Plugin:

```bash
kubectl describe node <npu-node>
```

Look for the expected `rebellions.ai/*` resources under Capacity and Allocatable.
Allocatable includes devices already requested by Pods; compare it with the
node's allocated resource requests before concluding that a device is free.

For DRA:

```bash
kubectl get deviceclasses
kubectl get resourceslices
```

Expect the selected DeviceClass and device inventory for your NPU nodes.

## 3. Run a workload

Follow [Run an NPU Pod](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/first_npu.html) or
[Request NPUs with DRA](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/request_npus_with_dra.html).
The final check is a running workload with access to its allocated device.

## Next steps

[Collect NPU metrics](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/collect_metrics.html) or
return to the [operator guides](../../README.md#guides).
