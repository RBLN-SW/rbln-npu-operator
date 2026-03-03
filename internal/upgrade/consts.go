package upgrade

// Label keys used to control and track node upgrades.
const (
	// UpgradeStateLabelKey indicates the node driver upgrade state.
	UpgradeStateLabelKey = "rebellions.ai/npu-driver-upgrade-state"
	// UpgradeSkipNodeLabelKey marks a node that should be skipped by the driver upgrade flow.
	UpgradeSkipNodeLabelKey = "rebellions.ai/npu-driver-upgrade.skip"
)

// Annotation keys used by the upgrade state machine.
const (
	// UpgradeRequestedAnnotationKey requests a driver upgrade for the node.
	UpgradeRequestedAnnotationKey = "rebellions.ai/npu-driver-upgrade-requested"
	// UpgradeWaitForSafeDriverLoadAnnotationKey indicates the driver waits for safe load on the node.
	UpgradeWaitForSafeDriverLoadAnnotationKey = "rebellions.ai/npu-driver-upgrade.driver-wait-for-safe-load"
	// UpgradeInitialStateAnnotationKey indicates the node was unschedulable at the beginning of the upgrade process.
	UpgradeInitialStateAnnotationKey = "rebellions.ai/npu-driver-upgrade.node-initial-state.unschedulable"
	// UpgradeWaitForPodCompletionStartTimeAnnotationKey stores the start time for wait-for-completion checks.
	UpgradeWaitForPodCompletionStartTimeAnnotationKey = "rebellions.ai/npu-driver-upgrade-wait-for-pod-completion-start-time"
	// UpgradeRequestorModeAnnotationKey indicates requestor driver upgrade mode is used for the node.
	UpgradeRequestorModeAnnotationKey = "rebellions.ai/npu-driver-upgrade-requestor-mode"
	// UpgradeValidationStartTimeAnnotationKey stores the start time for validation-required state.
	UpgradeValidationStartTimeAnnotationKey = "rebellions.ai/npu-driver-upgrade-validation-start-time"
	// UpgradePreRebootBootIDAnnotationKey stores node bootID before reboot is requested.
	UpgradePreRebootBootIDAnnotationKey = "rebellions.ai/npu-driver-upgrade-pre-reboot-boot-id"
	// UpgradeRebootRequestedAtAnnotationKey stores reboot request timestamp.
	UpgradeRebootRequestedAtAnnotationKey = "rebellions.ai/npu-driver-upgrade-reboot-requested-at"
	// UpgradeRebootPodNameAnnotationKey stores created reboot pod name for cleanup/debug.
	UpgradeRebootPodNameAnnotationKey = "rebellions.ai/npu-driver-upgrade-reboot-pod-name"
	// UpgradeRebootPostStartTimeAnnotationKey stores the start time for post-reboot stabilization checks.
	UpgradeRebootPostStartTimeAnnotationKey = "rebellions.ai/npu-driver-upgrade-reboot-post-start-time"
)

// Node upgrade states.
const (
	// UpgradeStateUnknown is used when the node has not been processed yet.
	UpgradeStateUnknown = ""
	// UpgradeStateUpgradeRequired means a node requires driver upgrade.
	UpgradeStateUpgradeRequired = "upgrade-required"
	// UpgradeStateCordonRequired means the node needs to be cordoned.
	UpgradeStateCordonRequired = "cordon-required"
	// UpgradeStateWaitForJobsRequired means the node waits for selected jobs to complete.
	UpgradeStateWaitForJobsRequired = "wait-for-jobs-required"
	// UpgradeStatePodDeletionRequired means pod deletion is required before proceeding.
	UpgradeStatePodDeletionRequired = "pod-deletion-required"
	// UpgradeStateDrainRequired means the node requires drain.
	UpgradeStateDrainRequired = "drain-required"
	// UpgradeStatePodRestartRequired means the driver pod must be restarted.
	UpgradeStatePodRestartRequired = "pod-restart-required"
	// UpgradeStateRebootRequired means the node must be rebooted before validation.
	UpgradeStateRebootRequired = "reboot-required"
	// UpgradeStateRebootValidationRequired means reboot results must be validated.
	UpgradeStateRebootValidationRequired = "reboot-validation-required"
	// UpgradeStateRebootPostRequired means post-reboot pod stabilization is required.
	UpgradeStateRebootPostRequired = "reboot-post-required"
	// UpgradeStateValidationRequired means the new driver must be validated.
	UpgradeStateValidationRequired = "validation-required"
	// UpgradeStateUncordonRequired means the node is ready to be uncordoned.
	UpgradeStateUncordonRequired = "uncordon-required"
	// UpgradeStateDone means upgrade is complete and node is schedulable.
	UpgradeStateDone = "upgrade-done"
	// UpgradeStateFailed means the upgrade failed.
	UpgradeStateFailed = "upgrade-failed"
)

// managedUpgradeStates is the canonical ordered list of states used by metrics/logging and state accounting.
var managedUpgradeStates = []string{
	UpgradeStateUnknown,
	UpgradeStateDone,
	UpgradeStateUpgradeRequired,
	UpgradeStateCordonRequired,
	UpgradeStateWaitForJobsRequired,
	UpgradeStatePodDeletionRequired,
	UpgradeStateFailed,
	UpgradeStateDrainRequired,
	UpgradeStatePodRestartRequired,
	UpgradeStateRebootRequired,
	UpgradeStateRebootValidationRequired,
	UpgradeStateRebootPostRequired,
	UpgradeStateValidationRequired,
	UpgradeStateUncordonRequired,
}

// Internal constants used across the upgrade package.
const (
	// nodeNameFieldSelectorFmt is used in metav1.ListOptions to filter resources by node name.
	nodeNameFieldSelectorFmt = "spec.nodeName=%s"
	// nullString avoids duplicated null literal usage.
	nullString = "null"
	// trueString avoids duplicated true literal usage.
	trueString = "true"

	DriverEventReason = "RBLNDriverUpgrade"
)
