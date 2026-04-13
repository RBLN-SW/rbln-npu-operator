package components

const (
	driverManagerName                         = "rbln-driver"
	driverManagerAppNameLabelKey              = "app.kubernetes.io/name"
	driverManagerAppLabelKey                  = "app.kubernetes.io/component"
	driverManagerNodePoolLabelKey             = "rebellions.ai/driver-node-pool"
	driverManagerInstanceLabelKey             = "rebellions.ai/driver-instance"
	driverManagerDeployLabelKey               = "rebellions.ai/npu.deploy.driver"
	driverManagerInitContainer                = "k8s-driver-manager"
	driverManagerContainer                    = "rbln-driver-container"
	driverManagerCommand                      = "driver-manager"
	driverManagerSyncDriverLabel              = "reconcile-driver-state"
	driverConfigDigestEnv                     = "DRIVER_CONFIG_DIGEST"
	driverLastAppliedHashAnnotation           = "rebellions.ai/last-applied-hash"
	driverInstallerCommand                    = "/opt/rebellions/bin/rbln-driver"
	driverInstallerInitArg                    = "init"
	startupProbeConfigMapSuffix               = "startup-probe"
	startupProbeScriptName                    = "startup-probe.sh"
	startupProbeScriptPath                    = "/usr/local/bin/rbln-startup-probe.sh"
	driverManagerStartupProbePeriodSeconds    = 10
	driverManagerStartupProbeTimeoutSeconds   = 120
	driverManagerStartupProbeFailureThreshold = 60
	hostDriverVolumeName                      = "host-driver"
	hostDriverPath                            = "/run/rbln/driver"
	hostRootVolumeName                        = "host-root"
	hostRootPath                              = "/"
	hostDevVolumeName                         = "host-dev"
	hostDevPath                               = "/dev"
)

func ptr[T any](v T) *T {
	return &v
}
