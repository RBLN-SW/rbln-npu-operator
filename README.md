# RBLN NPU Operator

**Run and manage Rebellions NPUs on Kubernetes and OpenShift.**

[Get started](#get-started) · [Guides](#guides) · [Documentation](https://docs.rbln.ai/latest/software/system_management/kubernetes/index.html) · [Releases](https://github.com/RBLN-SW/rbln-npu-operator/releases)

The operator deploys the components needed to run NPU workloads, manage drivers,
and collect metrics. Configure the cluster through `RBLNClusterPolicy` and
manage driver versions through `RBLNDriver`.

## Get started

1. [Install on Kubernetes](https://docs.rbln.ai/latest/software/system_management/kubernetes/install_npu_operator.html) or
   [OpenShift](https://docs.rbln.ai/latest/software/system_management/openshift/about_openshift.html).
   The guides cover supported environments, driver images, and registry access.
2. [Check the deployment](docs/guides/check-deployment.md).
3. [Run your first NPU Pod](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/first_npu.html).

Already installed? Start with the deployment check or your workload guide below.
Use [release-matched charts, images, and documentation](docs/guides/choose-release.md).

## Guides

| I want to… | Guide |
| --- | --- |
| Request NPUs with a ResourceClaim | [Use DRA](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/request_npus_with_dra.html) |
| Divide NPUs into virtual functions | [Configure NPU partitions](docs/guides/configure-partitions.md) |
| Attach an NPU to a virtual machine | [Use KubeVirt](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/kubevirt_passthrough.html) |
| Collect NPU metrics | [Connect Prometheus](https://docs.rbln.ai/latest/software/system_management/kubernetes/tutorials/collect_metrics.html) |
| Update NPU drivers | [Upgrade drivers](https://docs.rbln.ai/latest/software/system_management/kubernetes/upgrade_driver/index.html) |
| Run different driver versions on node groups | [Configure driver versions](docs/guides/configure-drivers.md) |
| Upgrade or remove the operator | [Upgrade](docs/guides/upgrade-chart.md) · [Uninstall](https://docs.rbln.ai/latest/software/system_management/kubernetes/uninstall_npu_operator.html) |
| Investigate an installation or workload problem | [Check the deployment](docs/guides/check-deployment.md) · [Collect logs](docs/guides/collect-logs.md) |

For chart settings and sample values, see the
[Helm chart guide](deployments/rbln-npu-operator/README.md).
For an existing installation moving to family-specific driver configurations,
read the [migration guide](docs/rolling-driver-migration.md).

## Choose an allocation method

| Workload | Request method | Component |
| --- | --- | --- |
| Container | `resources.limits` | Device Plugin |
| Container | `ResourceClaim` | DRA Driver |
| KubeVirt VM | `hostDevices` resource name | Sandbox Device Plugin |
| KubeVirt VM | `ResourceClaim` | DRA Driver |

DRA requires Kubernetes 1.34 or later. The operator rejects configurations that
enable DRA together with either device plugin. Follow the relevant guide before
switching allocation methods.

## Components

| Component | Role |
| --- | --- |
| [Device Plugin](https://github.com/RBLN-SW/k8s-device-plugin) | Exposes NPU resources to Pods |
| [DRA Driver](https://github.com/RBLN-SW/rbln-k8s-dra-driver) | Allocates NPUs through ResourceClaims |
| [Device Config Manager](docs/guides/configure-partitions.md) | Configures NPU partitions and driver bindings |
| [Driver Manager](https://docs.rbln.ai/latest/software/system_management/kubernetes/driver_installation.html) | Prepares nodes for driver installation and replacement |
| [Metrics Exporter](https://github.com/RBLN-SW/rbln-metrics-exporter) | Provides NPU metrics for Prometheus |
| [NPU Feature Discovery](https://github.com/RBLN-SW/rbln-npu-feature-discovery) | Publishes NPU information through node labels |
| [Sandbox Device Plugin](https://docs.rbln.ai/latest/software/system_management/kubernetes/sandboxed_workloads.html) | Exposes VFIO devices to KubeVirt VMs |

See the [system overview](https://docs.rbln.ai/latest/software/system_management/kubernetes/index.html) for the full stack,
including the Container Toolkit, system management daemon, and validators.

## Development

Start with [Contributing](CONTRIBUTING.md) to build and test the operator.

## Support

Open a [GitHub issue](https://github.com/RBLN-SW/rbln-npu-operator/issues) for bugs or feature requests.
For deployment-specific help, contact your Rebellions support representative.

[License](LICENSE)
