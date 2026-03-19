package components

const (
	DaemonSetReady    = "DaemonSetReady"
	DaemonSetNotReady = "DeamonSetNotReady"
	DaemonSetNotFound = "DeamonSetNotFound"

	DaemonSetPodsNotReady = "DaemonSetAllPodsNotReady"
	DaemonSetAllPodsReady = "DaemonSetAllPodsReady"

	validationsVolumeName = "run-rbln-validations"
	validationsMountPath  = "/run/rbln/validations"
)
