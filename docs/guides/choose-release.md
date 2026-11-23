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

## Release composition

The release runner reads [the artifact declaration](../../.piper/release.yaml)
from the release commit. By default it selects the highest SemVer base in
Harbor and then the highest numeric RC for that base. To select another base,
set an entry such as `componentVersions: {devicePlugin: v0.6.0}`. Set an
operand to `stable` to reuse its chart default. The driver container, SMD and
Container Toolkit keep their existing version policy.

The release asset `release-input.json` records the exact selected image tags,
digests and chart OCI digest. The Helm default values, OLM related images and
CR examples, and component table derive from that input. A new operand set can
receive a new Operator RC on the same source commit. GA requires the matching
RC's Piper status and the same image digests at the public release tags.

To package just the chart locally from the frozen input, use a separate job
directory for the result and package:

```sh
make release-artifact ARTIFACT_NAME=chart ARTIFACT_KIND=helm-chart \
  RELEASE_INPUT=/work/release-input.json RELEASE_RESULT=/work/chart-result.json
```

The package target changes values in a temporary copy. The registry publisher
uses the returned path and verifies the uploaded chart layer before attaching
the same bytes to the release.
