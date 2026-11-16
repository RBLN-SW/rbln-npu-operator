# Configure driver versions

Choose the driver version for each NPU family using this checkout's Helm chart.
For an installed release, use the chart and documentation for that release.

## Before you start

- Prepare the image pull secret described in the [installation guide](https://docs.rbln.ai/latest/software/system_management/kubernetes/driver_installation.html).
- Confirm that NFD has applied `rebellions.ai/npu.family` to the target nodes.
- Check that driver images exist for each target family, OS, and kernel.

If you are moving an existing installation to family-specific driver pools,
read the [migration guide](../rolling-driver-migration.md) before applying changes.
Changing driver ownership or versions can interrupt NPU workloads.

## 1. Choose the family versions

The chart disables driver deployment by default. Merge the following into your
existing installation values, replacing the version placeholders:

```yaml
driver:
  enabled: true
  deployDefault: false
  instances:
    atom:
      image:
        tag: "<atom-driver-version>"
      nodeSelector:
        rebellions.ai/npu.family: atom
    rebel100:
      image:
        tag: "<rebel100-driver-version>"
      nodeSelector:
        rebellions.ai/npu.family: rebel100
```

Use the [chart values](../../deployments/rbln-npu-operator/values.yaml) for this
release's defaults. A family with no matching nodes has no driver Pods.

Each instance needs its own node selector. Change
`driver.instances.<family>.image.tag` to update that family; `driver.image.tag`
does not override an instance's explicit version.

Helm keeps built-in instances when you omit them. Set an unwanted instance to
`null` to remove it, after reviewing the effect on its nodes. Use
[sample values](../../deployments/rbln-npu-operator/sample-values-MultiDriver.yaml)
when configuring additional node groups.

## 2. Check image availability

This chart uses family-specific driver image paths:

```text
<registry>/<organization>/<family>/rbln-driver:<version>-<kernel>-<os>
```

Publish or mirror all required images before updating the operator or driver
configuration. Keep the registry credentials available in the operator namespace.

## 3. Review and apply

Render the chart using your complete values file before installing or upgrading:

```bash
helm template rbln-npu-operator ./deployments/rbln-npu-operator \
  --namespace rbln-system -f <values-file>
```

If chart dependencies are missing, run `make helm-deps` first. Inspect each
rendered `RBLNDriver` for the intended version and node selector. Apply the
configuration through the [installation](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html)
or [driver upgrade](https://docs.rbln.ai/latest/software/system_management/kubernetes/upgrade_driver/index.html) procedure.

## 4. Verify

```bash
kubectl get rblndrivers.rebellions.ai
kubectl get nodes -L rebellions.ai/npu.family,rebellions.ai/npu.driver.owner
kubectl get pods -n rbln-system -o wide
```

Confirm that each target node has the intended driver owner and Ready driver
Pods. Then [check the NPU deployment](check-deployment.md).
