package consts

// log level
const (
	LogLevelError = iota - 2
	LogLevelWarning
	LogLevelInfo
	LogLevelDebug
)

// NPU labels
const (
	RBLNWorkloadConfigLabelKey      = "rebellions.ai/npu.workload.config"
	RBLNWorkloadConfigContainer     = "container"
	RBLNWorkloadConfigVMPassthrough = "vm-passthrough"
	RBLNWorkloadConfigUnknown       = "unknown"
	RBLNPresentLabelKey             = "rebellions.ai/npu.present"
	NFDLabelPrefix                  = "feature.node.kubernetes.io/"
)

// Container runtimes
const (
	Containerd = "containerd"
	Docker     = "docker"
	CRIO       = "crio"
)

// Condition types
const (
	RBLNConditionTypeReady           = "Ready"
	RBLNConditionTypeComponentsReady = "ComponentsReady"
)

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

// DeviceMapping maps product card names to their device IDs
var DeviceMapping = map[string][]string{
	RBLNCardCA12: {"1120", "1121"},
	RBLNCardCA22: {"1220", "1221"},
	RBLNCardCA25: {"1250", "1251"},
	RBLNCardCR03: {"2030", "2031"},
}
