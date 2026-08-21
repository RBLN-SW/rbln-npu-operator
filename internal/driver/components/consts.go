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
	hostSysVolumeName                         = "host-sys"
	hostSysPath                               = "/sys"
	chrootTmpVolumeName                       = "chroot-tmp"
	driverReadyVolumeName                     = "rbln-driver-state"
	driverReadyDirEnvName                     = "RBLN_DRIVER_READY_DIR"
	driverReadyFileEnvName                    = "RBLN_DRIVER_READY_FILE"
	defaultDriverReadyDir                     = "/run/rbln/driver-state"
	defaultDriverReadyFile                    = "ready"
	rdsPresentLabelKey                        = "rebellions.ai/rds.present"
	rdsBindingEnvName                         = "RBLN_RDS_BINDING"
	rdsBindingEnabledValue                    = "enabled"
	rdsPoolNameSuffix                         = "-rds"
	labelValueTrue                            = "true"
	rdsBindConfigMapSuffix                    = "rds-bind-config"
	rdsBindConfigVolumeName                   = "rds-bind-config"
	rdsBindConfVolumeName                     = "rds-bind-conf"
	rdsBindConfigSelectInitContainer          = "rds-bind-config-select"
	rdsBindConfigMountDir                     = "/rds-bind-config"
	rdsBindConfWorkDir                        = "/rds-bind-conf"
	rdsBindConfContainerPath                  = "/etc/rebellions/rblnfs-bind.conf"
	rdsBindConfFileName                       = "rblnfs-bind.conf"
	// driverCtrReadyFile is the cross-component "driver container ready" marker
	// published under consts.ValidationsMountPath by the driver startup probe
	// and removed by its preStop hook.
	driverCtrReadyFile = ".driver-ctr-ready"

	smdName              = "rbln-smd"
	smdDaemonSetSuffix   = "smd"
	smdCommand           = "/opt/rebellions/bin/rbln_daemon"
	smdPortName          = "rbln-smd"
	smdPort              = 50051
	smdInitContainerName = "driver-validation"
	smdVarRunVolumeName  = "host-var-run"
	smdVarRunPath        = "/var/run"
	smdDebugVolumeName   = "host-debug"
	smdDebugPath         = "/sys/kernel/debug"
	smdLogVolumeName     = "host-log-rebellions"
	smdLogPath           = "/var/log/rebellions"
	// legacyRBLNDaemonName is the pre-rename DaemonSet the RBLNClusterPolicy
	// controller used to own; it binds the same host port as rbln-smd.
	// TODO(remove after two releases): drop together with the clusterpolicy
	// legacy cleanup once no supported upgrade path ships it.
	legacyRBLNDaemonName = "rbln-daemon"
)

func ptr[T any](v T) *T {
	return &v
}
