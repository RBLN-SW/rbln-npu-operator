package upgrade

import (
	"encoding/json"
	"testing"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestTeardownPatch(t *testing.T) {
	tests := map[string]struct {
		releaseCordon bool
		wantSpec      bool
		wantClaimNull bool
	}{
		"bookkeeping only":            {releaseCordon: false},
		"release lifts cordon, claim": {releaseCordon: true, wantSpec: true, wantClaimNull: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			raw, err := TeardownPatch(tc.releaseCordon)
			if err != nil {
				t.Fatalf("TeardownPatch: %v", err)
			}
			var patch map[string]any
			if err := json.Unmarshal(raw, &patch); err != nil {
				t.Fatalf("patch is not JSON: %v", err)
			}
			meta := patch["metadata"].(map[string]any)
			if v, ok := meta["labels"].(map[string]any)[UpgradeStateLabelKey]; !ok || v != nil {
				t.Fatal("state label must be nulled")
			}
			annotations := meta["annotations"].(map[string]any)
			for _, key := range teardownAnnotationKeys {
				if v, ok := annotations[key]; !ok || v != nil {
					t.Fatalf("annotation %s must be nulled", key)
				}
			}
			if _, requested := annotations[UpgradeRequestedAnnotationKey]; requested {
				t.Fatal("the administrator's upgrade-requested annotation is not the rollout's to drop")
			}
			_, spec := patch["spec"]
			if spec != tc.wantSpec {
				t.Fatalf("spec present = %v, want %v", spec, tc.wantSpec)
			}
			_, claimNull := annotations[consts.DriverManagerCordonClaimAnnotation]
			_, markNull := annotations[consts.DriverManagerEvictionBlockedAnnotation]
			if claimNull != tc.wantClaimNull || markNull != tc.wantClaimNull {
				t.Fatalf("claim nulled = %v, mark nulled = %v, want %v (claim goes with the cordon, never alone)",
					claimNull, markNull, tc.wantClaimNull)
			}
		})
	}
}
