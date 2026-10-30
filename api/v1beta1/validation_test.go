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

package v1beta1

import (
	"strings"
	"testing"
)

func TestRBLNClusterPolicySpec_Validate(t *testing.T) {
	cases := map[string]struct {
		spec      RBLNClusterPolicySpec
		errSubstr string
	}{
		"empty workloadType is accepted (CRD default populates container)": {
			spec: RBLNClusterPolicySpec{},
		},
		"container workload is accepted": {
			spec: RBLNClusterPolicySpec{WorkloadType: "container"},
		},
		"vm-passthrough workload is accepted": {
			spec: RBLNClusterPolicySpec{WorkloadType: "vm-passthrough"},
		},
		"rejects unknown workloadType": {
			spec:      RBLNClusterPolicySpec{WorkloadType: "bare-metal"},
			errSubstr: "workloadType",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.spec.Validate()
			if tc.errSubstr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Fatalf("expected error containing %q, got %q", tc.errSubstr, err.Error())
			}
		})
	}
}
