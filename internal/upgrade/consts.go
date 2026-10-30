/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade

const (
	// These keys are persisted on nodes and consumed by existing integrations.
	// Internal refactors must retain their spelling and interpretation.
	UpgradeStateLabelKey = "rebellions.ai/npu-driver-upgrade-state"
	// Only the exact value "true" excludes a node from admission.
	UpgradeSkipNodeLabelKey = "rebellions.ai/npu-driver-upgrade.skip"
	// A "true" request survives teardown and is consumed by queue processing.
	UpgradeRequestedAnnotationKey = "rebellions.ai/npu-driver-upgrade-requested"
	// Any nonempty value asks the rollout to release the safe-load gate.
	UpgradeWaitForSafeDriverLoadAnnotationKey = "rebellions.ai/npu-driver-upgrade.driver-wait-for-safe-load"
	// Records a foreign cordon observed at the cordon step. Its presence also
	// prevents a claim-less legacy cordon from being treated as operator-owned.
	UpgradeInitialStateAnnotationKey = "rebellions.ai/npu-driver-upgrade.node-initial-state.unschedulable"
	// Presence, including an empty value, delegates final uncordon to a requestor.
	UpgradeRequestorModeAnnotationKey = "rebellions.ai/npu-driver-upgrade-requestor-mode"
	// Start times are Unix seconds and persist across operator restarts.
	UpgradeWaitForPodCompletionStartTimeAnnotationKey = "rebellions.ai/npu-driver-upgrade-wait-for-pod-completion-start-time"
	UpgradeValidationStartTimeAnnotationKey           = "rebellions.ai/npu-driver-upgrade-validation-start-time"
	UpgradePodRestartStartTimeAnnotationKey           = "rebellions.ai/npu-driver-upgrade-pod-restart-start-time"
	// Failure and skip diagnosis belongs to one attempt and is cleared on retry.
	UpgradeFailureReasonAnnotationKey = "rebellions.ai/npu-driver-upgrade-failure-reason"
	UpgradeFailureStepAnnotationKey   = "rebellions.ai/npu-driver-upgrade-failure-step"
	UpgradeSkipReasonAnnotationKey    = "rebellions.ai/npu-driver-upgrade-skip-reason"
	// Stores DRIVER_CONFIG_DIGEST when a parked node is first observed. A
	// changed digest can then wake it without retrying the failed revision.
	UpgradeAttemptedRevisionAnnotationKey = "rebellions.ai/npu-driver-upgrade-attempted-revision"
)

const (
	UpgradeStateUnknown             = ""
	UpgradeStateUpgradeRequired     = "upgrade-required"
	UpgradeStateCordonRequired      = "cordon-required"
	UpgradeStateWaitForJobsRequired = "wait-for-jobs-required"
	UpgradeStatePodDeletionRequired = "pod-deletion-required"
	UpgradeStatePodRestartRequired  = "pod-restart-required"
	UpgradeStateValidationRequired  = "validation-required"
	UpgradeStateUncordonRequired    = "uncordon-required"
	UpgradeStateDone                = "upgrade-done"
	// A failed driver keeps its cordon and parallelism slot until recovery.
	UpgradeStateFailed = "upgrade-failed"
	// Eviction stopped before replacing the driver. The node can return to
	// service on that driver, and no longer occupies a parallelism slot.
	UpgradeStateSkipped                   = "upgrade-skipped"
	DefaultValidationTimeoutSeconds int64 = 600
	MaxPodRestartCount              int32 = 10
	nodeNameFieldSelectorFmt              = "spec.nodeName=%s"
	trueString                            = "true"
)

var (
	managedUpgradeStates       = []string{UpgradeStateUnknown, UpgradeStateUpgradeRequired, UpgradeStateCordonRequired, UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired, UpgradeStatePodRestartRequired, UpgradeStateValidationRequired, UpgradeStateUncordonRequired, UpgradeStateDone, UpgradeStateFailed, UpgradeStateSkipped}
	badContainerWaitingReasons = map[string]bool{"CrashLoopBackOff": true, "ImagePullBackOff": true, "ErrImagePull": true, "ImageInspectError": true, "CreateContainerConfigError": true, "CreateContainerError": true, "RunContainerError": true, "InvalidImageName": true}
)
