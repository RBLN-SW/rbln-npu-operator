# Configure NPU partitions

Choose whole NPUs or SR-IOV virtual functions for a node using Device Config Manager.
Use [matching operator and component releases](choose-release.md).

## Before you start

- Confirm that the hardware, driver, and `rbln-smd` support the requested partition mode.
- Plan for NPU workload eviction. Stop affected VMs and release their allocations.
- Before first enabling the manager, review partition labels on every managed NPU
  node. An absent label means `pf`, which removes existing VFs.

## 1. Choose the node's partition mode

| Label value | Result |
| --- | --- |
| `pf` | Whole NPUs; no VFs |
| `vf1` | One VF per physical NPU |
| `vf4` | Four VFs per physical NPU |

Only these values are supported. Replace an existing `none` label with `pf`.
To select four VFs per NPU:

```bash
export NPU_NODE='<node-name>'
kubectl describe node "$NPU_NODE"
kubectl label node "$NPU_NODE" rebellions.ai/npu.partition=vf4 --overwrite
```

Existing `rebellions.ai/npu.partition.<index>` labels can override the VF count
for individual NPUs; review them before applying the node default. Overrides
cannot mix whole-NPU and VF modes on one node and are ignored in `pf` mode.
If the manager is already enabled, label changes take effect immediately.

## 2. Enable Device Config Manager

Set the desired labels on all managed NPU nodes before enabling the manager.
Merge this into your installation values and follow the [chart upgrade procedure](upgrade-chart.md):

```yaml
deviceConfigManager:
  enabled: true
```

VF configuration requires the native driver and a reachable `rbln-smd`.

## 3. Verify the result

```bash
kubectl describe node "$NPU_NODE"
```

Check that `NPUPartitionReady` is `True` with reason `AsConfigured` and a message
matching the requested layout. A previous `True` condition alone does not confirm
the latest change finished. Then [check device availability](check-deployment.md#2-check-npu-availability).

For container workloads, request VF4 devices through `rebellions.ai/npu-vf4`
with the Device Plugin or `npu-vf4.rebellions.ai` with DRA. One requested device
means one VF.

If configuration is blocked, read the node condition's message and
[collect Device Config Manager logs](collect-logs.md).

To return to whole NPUs, plan another workload interruption, set
`rebellions.ai/npu.partition=pf` with the label command above, and repeat verification.
