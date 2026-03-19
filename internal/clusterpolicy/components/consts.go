package components

const (
	DaemonSetReady    = "DaemonSetReady"
	DaemonSetNotReady = "DeamonSetNotReady"
	DaemonSetNotFound = "DeamonSetNotFound"

	DaemonSetPodsNotReady = "DaemonSetAllPodsNotReady"
	DaemonSetAllPodsReady = "DaemonSetAllPodsReady"

	hostUsrBinVolumeName = "host-usr-bin"
	hostUsrBinPath       = "/usr/bin"

	validationsVolumeName = "run-rbln-validations"
	validationsMountPath  = "/run/rbln/validations"
)
