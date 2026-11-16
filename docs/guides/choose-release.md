# Choose matching charts, images, and documentation

Use one operator release as the starting point for a deployment. Component
repositories and `/latest/` documentation can describe newer behavior.

## 1. Identify the release

For an existing Helm installation, replace the release name and namespace:

```bash
helm list -n rbln-system
helm get values rbln-npu-operator -n rbln-system
kubectl get pods -n rbln-system \
  -o 'custom-columns=NAME:.metadata.name,IMAGES:.spec.containers[*].image,INIT-IMAGES:.spec.initContainers[*].image'
```

The chart version identifies the installation defaults. The Pod image tags show
what is deployed, including component overrides and Driver Manager init containers.
For OLM, use the installed ClusterServiceVersion and the deployed image tags.

## 2. Check the intended combination

Choose a chart version from the [published releases](https://github.com/RBLN-SW/rbln-npu-operator/releases)
and check the [support matrix](https://docs.rbln.ai/latest/supports/version_matrix.html)
for your NPU family, operating system, and Kubernetes or OpenShift environment.

Inspect the chosen chart before installing or upgrading:

```bash
export CHART_VERSION='<chart-version>'
helm show chart oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart \
  --version "$CHART_VERSION"
helm show values oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart \
  --version "$CHART_VERSION"
```

Review component image tags and `driver.instances` together with your own values.
An image override changes the combination supplied by the chart; check the
component's release notes before using one. A source checkout's `Chart.yaml`
may contain development version placeholders.

## 3. Read the matching instructions

- Use the documentation version associated with your SDK/system software release.
  The external documentation version and operator chart version are separate.
- In a component repository, select the Git tag matching its deployed image before
  reading `docs/` or applying examples. Follow release notes if tag names differ.
- Links to `main` describe the current source. `/latest/` describes the current
  published documentation; neither pins the instructions to an older deployment.

If a feature has no published release instructions yet, keep it within the
documented development workflow instead of combining it with older chart defaults.

Next: [install the operator](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html)
or [upgrade an existing chart](upgrade-chart.md).
