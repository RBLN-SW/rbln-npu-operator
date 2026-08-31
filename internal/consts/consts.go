package consts

// Verbosity levels for logr call sites. Severity is expressed by the method
// (Error vs Info), never by V: V(0) carries state changes and warnings,
// VDebug per-reconcile detail. Messages start with a capital letter, carry
// no trailing period, and never embed a severity prefix such as "WARNING:".
//
// An error value always travels under the key "error" -- the key zapr forces
// on Error() records, and therefore the only one that can also serve the
// handled transients logged at Info. Severity is the level's job, not the
// key's: a second key would only mean every "what failed" query has to match
// both or silently miss half the records.
const VDebug = 1

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

	// RBLNDeploySmdLabelKey gates rbln-smd pods, and is the eviction handle
	// k8s-driver-manager flips to paused-for-driver-upgrade on every driver pod
	// start to bounce that node's smd pod. The key is hardcoded in that binary,
	// so a driver-manager older than the release that introduced it never flips
	// this label and leaves the node's smd pod stranded on a stale image. That
	// is why the two ship as a version pair — see the chart's
	// driver.manager.image.tag.
	RBLNDeploySmdLabelKey = "rebellions.ai/npu.deploy.rbln-smd"

	// RBLNDeployRBLNDaemonLabelKey is the pre-rename spelling of
	// RBLNDeploySmdLabelKey. Nothing selects on it; it exists only so upgraded
	// nodes get it swept off instead of carrying a dead label forever.
	RBLNDeployRBLNDaemonLabelKey = "rebellions.ai/npu.deploy.rbln-daemon"

	NFDLabelPrefix = "feature.node.kubernetes.io/"

	// RBLNDriverOwnerLabelKey names the single RBLNDriver that owns a node's
	// driver DaemonSet; DaemonSets select on it instead of user selectors.
	// Only npu.deploy.driver=true nodes are in scope, and the label is absent
	// while no driver claims the node. User selectors must not set it.
	RBLNDriverOwnerLabelKey = "rebellions.ai/npu.driver.owner"

	// RBLNNPUFamilyLabelKey carries the node's NPU product family (values in
	// NPUFamilies), applied by the operator-managed NodeFeatureRule for use
	// in RBLNDriver nodeSelectors.
	RBLNNPUFamilyLabelKey = "rebellions.ai/npu.family"

	RBLNDeployDriverPreInstalled = "pre-installed"
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
	RBLNConditionReasonSmdNotReady             = "SmdNotReady"
	RBLNConditionReasonConflictingSelector     = "ConflictingNodeSelector"
	RBLNConditionReasonFamilyLabelMissing      = "DriverFamilyLabelMissing"
	RBLNConditionReasonImageNotFound           = "DriverImageNotFound"
)

// Event reasons — observability contract, keep stable. A reason string means
// one thing across both CRs.
const (
	RBLNEventReasonDriverUpgradeStarted   = "DriverUpgradeStarted"
	RBLNEventReasonNodeDrained            = "NodeDrained"
	RBLNEventReasonNodeDrainFailed        = "NodeDrainFailed"
	RBLNEventReasonDriverUpgradeCompleted = "DriverUpgradeCompleted"
	RBLNEventReasonDriverUpgradeFailed    = "DriverUpgradeFailed"
	RBLNEventReasonDriverUpgradeSkipped   = "DriverUpgradeSkipped"
	RBLNEventReasonDriverUpgradePodStuck  = "DriverUpgradePodStuck"
	RBLNEventReasonComponentApplyFailed   = "ComponentApplyFailed"
	RBLNEventReasonDriverInstallFailed    = "DriverInstallFailed"
	RBLNEventReasonDriverReady            = "DriverReady"
	RBLNEventReasonDriverNodeUncovered    = "DriverNodeUncovered"
	RBLNEventReasonDriverOwnerChanged     = "DriverOwnerChanged"
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

// NPU feature discovery constants
const (
	RBLNFeatureDiscoveryName = "npu-feature-discovery"
)

// NPU family NodeFeatureRule constants
const (
	RBLNNPUFamilyRuleName = "npu-family"
)

// NPUFamily ties one NPU product family (an RBLNNPUFamilyLabelKey value) to
// its PCI device IDs under vendor RBLNVendorCode.
type NPUFamily struct {
	Name      string
	DeviceIDs []string
}

// NPUFamilies is the only device-ID → family mapping in the operator; extend
// it when new device IDs ship. Slice order is the NodeFeatureRule rule order
// and must stay deterministic so reconciles are no-ops. All rules share one
// label key, so a (hardware-premise-impossible) node carrying devices from
// two families would silently get the last matching family in this order.
var NPUFamilies = []NPUFamily{
	{Name: "atom", DeviceIDs: []string{"1220", "1221", "1250", "1251"}},
	{Name: "rebel100", DeviceIDs: []string{"2030", "2031", "2130", "2131"}},
}

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
