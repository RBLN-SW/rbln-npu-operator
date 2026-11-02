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

// A node decision uses values observed during this pass. It cannot read the
// API, change an object or consume another node's admission slot. The executor
// applies the resulting ordered effects and stops that node on a write error.
// Admission requires a fresh cluster-wide budget and conditional node writes;
// eviction and validation require I/O. Those operations stay in their handlers.
type driverObservation struct {
	managed, configMatches, templateMatches bool
	present, terminating, ready, failing    bool
}

type nodeObservation struct {
	driver                       driverObservation
	requested, safeLoad          bool
	cordoned, claimed, requestor bool
	validationEnabled            bool
}

type nodeOperation uint8

const (
	enqueueAttempt nodeOperation = iota
	releaseScheduling
	publishNodeState
	acknowledgeUncordon
	clearInitialCordon
	allowDriverLoad
	replaceDriverPod
	clearRestartDeadline
	recordRestartFailure
	finishAttempt
)

type nodeIntent struct {
	operation nodeOperation
	state     string
	cause     string
}

type nodePlan []nodeIntent

func decideNode(state string, observed nodeObservation) nodePlan {
	switch state {
	case UpgradeStateUnknown, UpgradeStateDone:
		return decideIdleNode(state, observed)
	case UpgradeStatePodRestartRequired:
		return decideReplacement(observed)
	case UpgradeStateUncordonRequired:
		if !observed.requestor {
			return nodePlan{{operation: releaseScheduling}, {operation: acknowledgeUncordon}}
		}
	}
	return nil
}

func decideIdleNode(state string, observed nodeObservation) nodePlan {
	driverChanged := observed.driver.managed && !observed.driver.configMatches
	cause := ""
	switch {
	case observed.requested:
		cause = "manual request"
	case observed.safeLoad:
		cause = "driver waiting for safe load"
	case driverChanged:
		cause = "driver configuration changed"
	}
	if cause != "" {
		return nodePlan{{operation: enqueueAttempt, cause: cause}}
	}
	var plan nodePlan
	if observed.cordoned && observed.claimed && !observed.requestor {
		plan = append(plan, nodeIntent{operation: releaseScheduling})
	}
	if state == UpgradeStateUnknown {
		plan = append(plan, nodeIntent{operation: publishNodeState, state: UpgradeStateDone})
	}
	return plan
}

func decideReplacement(observed nodeObservation) nodePlan {
	driver := observed.driver
	if !driver.managed || !driver.configMatches || !driver.templateMatches {
		if driver.present && !driver.terminating {
			return nodePlan{{operation: replaceDriverPod}}
		}
		return nil
	}
	plan := nodePlan{{operation: allowDriverLoad}}
	switch {
	case driver.ready:
		plan = append(plan, nodeIntent{operation: clearRestartDeadline})
		if observed.validationEnabled {
			return append(plan, nodeIntent{operation: publishNodeState, state: UpgradeStateValidationRequired})
		}
		return append(plan, nodeIntent{operation: finishAttempt})
	case driver.failing:
		return append(plan, nodeIntent{operation: recordRestartFailure}, nodeIntent{operation: clearRestartDeadline})
	default:
		return plan
	}
}

func completionPlan(observed nodeObservation) nodePlan {
	target := UpgradeStateUncordonRequired
	if observed.cordoned && !observed.claimed && !observed.requestor {
		target = UpgradeStateDone
	}
	plan := nodePlan{{operation: publishNodeState, state: target}}
	if target == UpgradeStateDone || observed.requestor {
		plan = append(plan, nodeIntent{operation: clearInitialCordon})
	}
	return plan
}
