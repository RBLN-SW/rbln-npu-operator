package consts

// log level
const (
	LogLevelError = iota - 2
	LogLevelWarning
	LogLevelInfo
	LogLevelDebug
)

// NFD label keys
const (
	NFDOSReleaseIDLabelKey  = "feature.node.kubernetes.io/system-os_release.ID"
	NFDOSVersionIDLabelKey  = "feature.node.kubernetes.io/system-os_release.VERSION_ID"
	NFDKernelLabelKey       = "feature.node.kubernetes.io/kernel-version.full"
	NFDDevicePCILabelKey    = "feature.node.kubernetes.io/pci-1eff.present"
	NFDDevicePCIAltLabelKey = "feature.node.kubernetes.io/pci-1200_1eff.present"
)

// NPU labels
const (
	RBLNWorkloadConfigLabelKey      = "rebellions.ai/npu.workload.config"
	RBLNWorkloadConfigContainer     = "container"
	RBLNWorkloadConfigVMPassthrough = "vm-passthrough"
	RBLNWorkloadConfigUnknown       = "unknown"
	RBLNPresentLabelKey             = "rebellions.ai/npu.present"
	RBLNDeploySkipLabelKey          = "rebellions.ai/npu.deploy.skip"
	RBLNDeployDriverLabelKey        = "rebellions.ai/npu.deploy.driver"
	RBLNDeployRBLNDaemonLabelKey    = "rebellions.ai/npu.deploy.rbln-daemon"
	NFDLabelPrefix                  = "feature.node.kubernetes.io/"

	RBLNDeployDriverPreInstalled = "pre-installed"
)

// NPU partitioning labels (admin contract) and partition mode tokens
const (
	RBLNPartitionLabelKey         = "rebellions.ai/npu.partition"
	RBLNPartitionIndexLabelPrefix = "rebellions.ai/npu.partition."

	RBLNPartitionModeVF1  = "vf1"
	RBLNPartitionModeVF4  = "vf4"
	RBLNPartitionModeNone = "none"
)

// Container runtimes
const (
	Containerd = "containerd"
	Docker     = "docker"
	CRIO       = "crio"
)

// Condition types
const (
	RBLNConditionTypeReady = "Ready"
)

// Condition types (driver)
const (
	RBLNConditionTypeError = "Error"
)

// Top-level CR state values (string form persisted in .status.state)
const (
	RBLNStateReady    = "ready"
	RBLNStateNotReady = "notReady"
	RBLNStateIgnored  = "ignored"
)

// Condition reasons
const (
	RBLNConditionReasonNFDNotFound             = "NodeFeatureDiscoveryNotFound"
	RBLNConditionReasonNoRBLNNodes             = "NoRBLNNodesDiscovered"
	RBLNConditionReasonPolicyIgnored           = "PolicyIgnored"
	RBLNConditionReasonAllDriverPoolsReady     = "AllDriverPoolsReady"
	RBLNConditionReasonAllActiveWorkloadsReady = "AllActiveWorkloadsReady"
	RBLNConditionReasonWorkloadUncovered       = "WorkloadUncovered"
	RBLNConditionReasonWorkloadProgressing     = "WorkloadProgressing"
	RBLNConditionReasonError                   = "Error"
	RBLNConditionReasonReconcileFailed         = "ReconcileFailed"
	RBLNConditionReasonMissingClusterPolicy    = "MissingClusterPolicy"
	RBLNConditionReasonInvalidSpec             = "InvalidSpec"
	RBLNConditionReasonDriverPoolNotReady      = "DriverPoolNotReady"
	RBLNConditionReasonConflictingSelector     = "ConflictingNodeSelector"
)

// Event reasons — observability contract, keep stable. A reason string means
// one thing across both CRs.
const (
	RBLNEventReasonDriverUpgradeStarted   = "DriverUpgradeStarted"
	RBLNEventReasonNodeDrained            = "NodeDrained"
	RBLNEventReasonNodeDrainFailed        = "NodeDrainFailed"
	RBLNEventReasonDriverUpgradeCompleted = "DriverUpgradeCompleted"
	RBLNEventReasonDriverUpgradeFailed    = "DriverUpgradeFailed"
	RBLNEventReasonComponentApplyFailed   = "ComponentApplyFailed"
	RBLNEventReasonDriverInstallFailed    = "DriverInstallFailed"
	RBLNEventReasonDriverReady            = "DriverReady"
)

// Base name prefix for all RBLN components
const RBLNBaseName = "rbln"

// Device plugin constants
const (
	RBLNDevicePluginName     = "device-plugin"
	RBLNDRAKubeletPluginName = "dra-kubelet-plugin"
	DeviceTypeAccelerator    = "accelerator"
	RBLNVendorCode           = "1eff"
	RBLNDriverName           = "rebellions"
	RBLNCardCA12             = "RBLN-CA12"
	RBLNCardCA22             = "RBLN-CA22"
	RBLNCardCA25             = "RBLN-CA25"
	RBLNCardCR03             = "RBLN-CR03"
)

// Sandbox device plugin constants
const (
	RBLNSandboxDevicePluginName = "sandbox-device-plugin"
	RBLNSandboxDriverName       = "vfio-pci"
)

// Metrics export constants
const (
	RBLNMetricExporterName = "metrics-exporter"
)

// Partition manager constants
const (
	RBLNPartitionManagerName = "partition-manager"
)

// RBLN daemon constants
const (
	RBLNDaemonName = "rbln-daemon"
)

// NPU feature discovery constants
const (
	RBLNFeatureDiscoveryName = "npu-feature-discovery"
)

// Container toolkit constants
const (
	RBLNContainerToolkitName = "container-toolkit"
)

// Validator constants
const (
	RBLNValidatorName = "operator-validator"
)

// VFIO constants
const (
	RBLNVFIOManagerName = "vfio-manager"
)

// Kubernetes recommended labels
const (
	LabelAppName      = "app.kubernetes.io/name"
	LabelAppComponent = "app.kubernetes.io/component"
	OperatorName      = "rbln-npu-operator"
)

// Shared volume constants
const (
	ValidationsVolumeName = "run-rbln-validations"
	ValidationsMountPath  = "/run/rbln/validations"
)

// PartitionReadyFileName is written by the partition-manager once VF
// partitioning is applied on the node; operand gates wait for it.
const PartitionReadyFileName = "partition-ready"
