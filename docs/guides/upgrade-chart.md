# Upgrade the Helm chart

Update the operator and its component configuration using a pinned chart version.
For changing only NPU driver versions, use the
[driver upgrade guide](https://docs.rbln.ai/latest/software/system_management/kubernetes/upgrade_driver/index.html).

## Before you start

- [Choose a compatible release](choose-release.md) and read its release notes.
- Keep your installation values, image credentials, release name, and namespace.
- If moving from a default driver to family-specific instances, complete the
  [migration review](../rolling-driver-migration.md).
- Review [component update strategies](configure-update-strategies.md) before
  changing images or settings used by active workloads.

## 1. Review the changes

Replace the version and values path. Render the intended release:

```bash
export CHART_VERSION='<chart-version>'
helm template rbln-npu-operator \
  oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart \
  --version "$CHART_VERSION" -n rbln-system -f <values-file>
```

Check component images, allocation method, and every `RBLNDriver` version and
node selector. Rendering does not execute hooks or verify installed CRDs.

## 2. Upgrade

```bash
helm upgrade rbln-npu-operator \
  oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart \
  --version "$CHART_VERSION" -n rbln-system -f <values-file> \
  --wait --timeout 10m
```

This chart enables `crds.upgrade.enabled` by default. Its pre-install/pre-upgrade
hook applies the CRDs bundled in the operator image. Keep the operator image
matched to the chart release.
Helm's normal handling of the `crds/` directory alone does not update existing CRDs.

If your GitOps configuration manages CRDs separately, apply the target release's
CRDs before updating custom resources and disable the hook in your values:

```yaml
crds:
  upgrade:
    enabled: false
```

## 3. Verify

Run the [deployment checks](check-deployment.md) and a small NPU workload.
If Helm fails in the CRD hook, inspect its Job, Pod events, and logs in the operator
namespace before retrying. Helm completion alone does not confirm every operand
or NPU workload is ready.

For removal, follow the [uninstallation guide](https://docs.rbln.ai/latest/software/system_management/kubernetes/uninstall_npu_operator.html).
Helm does not remove the CRD definitions; the removal guide covers custom resources
and operand cleanup.
