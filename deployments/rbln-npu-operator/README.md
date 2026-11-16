# RBLN NPU Operator Helm Chart

Deploy the NPU Operator and choose the components for your cluster.

[Install](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html) · [Values](values.yaml) · [Upgrade](../../docs/guides/upgrade-chart.md) · [NPU Operator](../../README.md)

## Get started

Follow the [Kubernetes installation guide](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html)
for supported environments, registry credentials, and a complete values file.
For OpenShift OLM, use the [OpenShift guide](https://docs.rbln.ai/latest/software/system_management/openshift/about_openshift.html).

Use a [release-matched chart and images](../../docs/guides/choose-release.md).
Inspect the chosen chart's defaults with:

```bash
export CHART_VERSION='<chart-version>'
helm show values oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart \
  --version "$CHART_VERSION"
```

This checkout disables driver deployment by default. To install drivers through
operator-managed containers, follow [Configure driver versions](../../docs/guides/configure-drivers.md).
That guide covers family selectors, image paths, and instance-specific version overrides.

## Choose a configuration

Use sample values from the same release as your chart and merge the settings you
need into your installation values. Review image versions and enabled components
before applying a sample.

| Task | Start here |
| --- | --- |
| Configure container workloads | [Container sample](sample-values-ContainerWorkload.yaml) |
| Configure VM passthrough | [Sandbox sample](sample-values-SandboxWorkload.yaml) |
| Use multiple driver versions or node groups | [Multi-driver sample](sample-values-MultiDriver.yaml) · [Driver guide](../../docs/guides/configure-drivers.md) |
| Control component Pod replacement | [Update strategies](../../docs/guides/configure-update-strategies.md) |
| Change component log levels | [Collect logs](../../docs/guides/collect-logs.md) |
| Upgrade the chart and CRDs | [Upgrade guide](../../docs/guides/upgrade-chart.md) |
| Move an existing default driver to family instances | [Migration guide](../../docs/rolling-driver-migration.md) |

For all keys and defaults, read [values.yaml](values.yaml). DRA and either device
plugin are alternative allocation methods; the operator rejects enabling them together.

## Preview a local chart change

From the repository root:

```bash
make helm-deps
helm template rbln-npu-operator ./deployments/rbln-npu-operator \
  --namespace rbln-system -f <values-file>
```

Inspect the rendered policy, driver selectors, and images before deployment.
A successful render does not check image availability or node readiness.

Next: [check the deployment](../../docs/guides/check-deployment.md), then choose
an [NPU workload guide](../../README.md#guides).
