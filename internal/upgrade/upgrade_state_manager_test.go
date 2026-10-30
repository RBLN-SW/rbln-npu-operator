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
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kfake "k8s.io/client-go/kubernetes/fake"
)

func TestManagerOptionsConfigureOptionalOperations(t *testing.T) {
	filter := func(corev1.Pod) bool { return true }
	for _, tc := range []struct {
		name                 string
		options              ManagerOptions
		deletion, validation bool
	}{
		{name: "disabled"},
		{name: "eviction", options: ManagerOptions{PodDeletionFilter: filter}, deletion: true},
		{name: "validation", options: ManagerOptions{ValidationPodSelector: "app=validator"}, validation: true},
		{name: "both", options: ManagerOptions{PodDeletionFilter: filter, ValidationPodSelector: "app=validator"}, deletion: true, validation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newTestManager(t)
			m := newStateManager(logr.Discard(), base.k8sClient, kfake.NewClientset(), nil, tc.options)
			if m.IsPodDeletionEnabled() != tc.deletion || m.IsValidationEnabled() != tc.validation {
				t.Fatalf("optional operation configuration: deletion=%v validation=%v", m.IsPodDeletionEnabled(), m.IsValidationEnabled())
			}
			if m.validationManager == nil {
				t.Fatal("a disabled validator still needs a no-op implementation")
			}
		})
	}
}

func TestDisabledValidationDoesNotReadPods(t *testing.T) {
	validator := NewValidationManager(nil, nil, "")
	if done, err := validator.Validate(context.Background(), &corev1.Node{}); err != nil || !done {
		t.Fatalf("disabled validation: done=%v, err=%v", done, err)
	}
}
