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

package controller

import (
	"testing"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// TestSummariseWorkloadStatuses pins the precedence used to derive the
// top-level CR state + condition reason from per-workload aggregate states:
//
//	uncovered (most actionable) → notReady / WorkloadUncovered
//	progressing                 → notReady / WorkloadProgressing
//	all ready or empty          → ready    / AllActiveWorkloadsReady
//
// These transitions are the user-facing contract of the workload-indexed
// status redesign, so the table covers every state combination that affects
// the outcome.
func TestSummariseWorkloadStatuses(t *testing.T) {
	tests := map[string]struct {
		workloads   []rblnv1beta1.RBLNWorkloadStatus
		wantState   string
		wantReason  string
		wantMessage string // exact match; "" when message is implementation-detail
	}{
		"all ready": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.WorkloadStateReady},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateReady},
			},
			wantState:   consts.RBLNStateReady,
			wantReason:  consts.RBLNConditionReasonAllActiveWorkloadsReady,
			wantMessage: "All workloads with NPU nodes are ready",
		},
		"all empty": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.WorkloadStateEmpty},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateEmpty},
			},
			wantState:  consts.RBLNStateReady,
			wantReason: consts.RBLNConditionReasonAllActiveWorkloadsReady,
		},
		"ready + empty": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.WorkloadStateReady},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateEmpty},
			},
			wantState:  consts.RBLNStateReady,
			wantReason: consts.RBLNConditionReasonAllActiveWorkloadsReady,
		},
		"uncovered wins over progressing": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 2, State: rblnv1beta1.WorkloadStateProgressing},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 1, State: rblnv1beta1.WorkloadStateUncovered},
			},
			wantState:   consts.RBLNStateNotReady,
			wantReason:  consts.RBLNConditionReasonWorkloadUncovered,
			wantMessage: "Uncovered workload(s): vm-passthrough(1 node)",
		},
		"progressing when no uncovered": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, ReadyCount: 1, ComponentCount: 3, State: rblnv1beta1.WorkloadStateProgressing},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateEmpty},
			},
			wantState:   consts.RBLNStateNotReady,
			wantReason:  consts.RBLNConditionReasonWorkloadProgressing,
			wantMessage: "Progressing workload(s): container(1/3 ready)",
		},
		"progressing with unequal counts carries the workload message": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, ReadyCount: 0, ComponentCount: 8, State: rblnv1beta1.WorkloadStateProgressing, Message: "0/8 components ready on 1 container node(s); 1 node(s) paused for driver install/upgrade: node-a"},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateEmpty},
			},
			wantState:   consts.RBLNStateNotReady,
			wantReason:  consts.RBLNConditionReasonWorkloadProgressing,
			wantMessage: "Progressing workload(s): container(0/8 components ready on 1 container node(s); 1 node(s) paused for driver install/upgrade: node-a)",
		},
		"progressing with every component ready carries the workload message": {
			workloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, ReadyCount: 6, ComponentCount: 6, State: rblnv1beta1.WorkloadStateProgressing, Message: "2 of 3 container node(s) paused for driver install/upgrade: node-a, node-b"},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.WorkloadStateEmpty},
			},
			wantState:   consts.RBLNStateNotReady,
			wantReason:  consts.RBLNConditionReasonWorkloadProgressing,
			wantMessage: "Progressing workload(s): container(2 of 3 container node(s) paused for driver install/upgrade: node-a, node-b)",
		},
		"empty input → ready": {
			workloads:  nil,
			wantState:  consts.RBLNStateReady,
			wantReason: consts.RBLNConditionReasonAllActiveWorkloadsReady,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotState, gotReason, gotMessage := summariseWorkloadStatuses(tc.workloads)
			if gotState != tc.wantState {
				t.Errorf("state: got %q, want %q", gotState, tc.wantState)
			}
			if gotReason != tc.wantReason {
				t.Errorf("reason: got %q, want %q", gotReason, tc.wantReason)
			}
			if tc.wantMessage != "" && gotMessage != tc.wantMessage {
				t.Errorf("message: got %q, want %q", gotMessage, tc.wantMessage)
			}
		})
	}
}
