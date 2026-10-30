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

import (
	"encoding/json"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// teardownAnnotationKeys is the node bookkeeping that goes with the state
// label whenever the rollout lets go of a node: autoUpgrade turned off, or the
// node leaving the driver's scope.
//
// The initial-state annotation has to go with the label. It records whether the
// node was unschedulable when the rollout cordoned it, and left behind it
// outlives the rollout that meant it: the next one would read this rollout's own
// leftover cordon as the administrator's and refuse to lift it forever. The
// timeout clocks are cleared only when their state completes, so a rollout
// paused inside one would hand the next rollout a stale epoch and an instant
// timeout. The attempt's bookkeeping is dropped on admission by
// clearParkedBookkeeping, but a node whose driver pod is already in sync goes
// straight to upgrade-done and never passes it.
//
// UpgradeRequestedAnnotationKey is deliberately absent. It is the
// administrator's instruction, not the rollout's bookkeeping: an attempt they
// asked for and did not get outlives the pause that interrupted it, and the
// next rollout consumes it on admission (ProcessUpgradeRequiredNodes).
//
// The cordon claim is absent too: it goes only with the cordon it marks (see
// TeardownPatch), and with autoUpgrade off the claim on a node may be
// k8s-driver-manager's, which is then holding the cordon legitimately.
var teardownAnnotationKeys = []string{
	UpgradeInitialStateAnnotationKey,
	UpgradeValidationStartTimeAnnotationKey,
	UpgradeWaitForPodCompletionStartTimeAnnotationKey,
	UpgradePodRestartStartTimeAnnotationKey,
	UpgradeSkipReasonAnnotationKey,
	UpgradeAttemptedRevisionAnnotationKey,
}

// TeardownPatch strips a node of the rollout's bookkeeping in one merge patch.
// With releaseCordon the same write lifts the cordon and clears the claim that
// marked it as the rollout's, so a node can never be left uncordoned while
// still claimed, or claimed while already back in service.
func TeardownPatch(releaseCordon bool) ([]byte, error) {
	annotations := make(map[string]any, len(teardownAnnotationKeys)+2)
	for _, key := range teardownAnnotationKeys {
		annotations[key] = nil
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels":      map[string]any{UpgradeStateLabelKey: nil},
			"annotations": annotations,
		},
	}
	if releaseCordon {
		annotations[consts.DriverManagerCordonClaimAnnotation] = nil
		annotations[consts.DriverManagerEvictionBlockedAnnotation] = nil
		patch["spec"] = map[string]any{"unschedulable": false}
	}
	return json.Marshal(patch)
}
