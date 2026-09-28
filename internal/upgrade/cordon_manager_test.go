package upgrade

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	testClaimKey   = consts.DriverManagerCordonClaimAnnotation
	testBlockedKey = consts.DriverManagerEvictionBlockedAnnotation
)

func cordonTestNode(unschedulable bool, state string, annotations map[string]string) *corev1.Node {
	labels := map[string]string{}
	if state != "" {
		labels[UpgradeStateLabelKey] = state
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Labels: labels, Annotations: annotations, ResourceVersion: "7"},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable},
	}
}

func getTestNode(t *testing.T, cs *k8sfake.Clientset) *corev1.Node {
	t.Helper()
	node, err := cs.CoreV1().Nodes().Get(context.Background(), "worker", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	return node
}

func TestCordonManagerCordon(t *testing.T) {
	tests := map[string]struct {
		node        *corev1.Node
		wantForeign bool
		wantPatched bool
		wantClaim   string
	}{
		"schedulable node is cordoned and claimed in one write": {
			node:        cordonTestNode(false, UpgradeStateCordonRequired, nil),
			wantPatched: true,
			wantClaim:   consts.OperatorCordonClaimValue,
		},
		// A claim a killed manual-mode run left on a node the administrator
		// then uncordoned by hand: overwritten, so the binary can never read
		// the rollout's cordon as its own.
		"stale driver-manager claim on a schedulable node is overwritten": {
			node:        cordonTestNode(false, UpgradeStateCordonRequired, map[string]string{testClaimKey: "driver", testBlockedKey: "PDB"}),
			wantPatched: true,
			wantClaim:   consts.OperatorCordonClaimValue,
		},
		"cordon already claimed by the operator is reclaimed without a write": {
			node:      cordonTestNode(true, UpgradeStateCordonRequired, map[string]string{testClaimKey: consts.OperatorCordonClaimValue}),
			wantClaim: consts.OperatorCordonClaimValue,
		},
		"cordon claimed by k8s-driver-manager is adopted, its blocked mark cleared": {
			node:        cordonTestNode(true, UpgradeStateCordonRequired, map[string]string{testClaimKey: "driver", testBlockedKey: "PDB webapp"}),
			wantPatched: true,
			wantClaim:   consts.OperatorCordonClaimValue,
		},
		"pre-rename claim is adopted too": {
			node:        cordonTestNode(true, UpgradeStateCordonRequired, map[string]string{testClaimKey: "true"}),
			wantPatched: true,
			wantClaim:   consts.OperatorCordonClaimValue,
		},
		// Not judged by the legacy rule either: cordon-required is excluded
		// from it, because the cordon may be an administrator's the rollout
		// has not looked at yet.
		"unclaimed cordon is the administrator's: reported, not touched": {
			node:        cordonTestNode(true, UpgradeStateCordonRequired, nil),
			wantForeign: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cs := k8sfake.NewClientset(tc.node)
			patches := 0
			cs.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				patches++
				return false, nil, nil
			})
			m := NewCordonManager(cs)

			foreign, err := m.Cordon(context.Background(), tc.node.DeepCopy())
			if err != nil {
				t.Fatalf("Cordon: %v", err)
			}
			if foreign != tc.wantForeign {
				t.Fatalf("foreign = %v, want %v", foreign, tc.wantForeign)
			}
			if (patches > 0) != tc.wantPatched {
				t.Fatalf("patches = %d, wantPatched %v", patches, tc.wantPatched)
			}

			updated := getTestNode(t, cs)
			if !tc.wantForeign && !updated.Spec.Unschedulable {
				t.Fatal("node must be unschedulable after Cordon")
			}
			if got := updated.Annotations[testClaimKey]; got != tc.wantClaim {
				t.Fatalf("claim = %q, want %q", got, tc.wantClaim)
			}
			if _, left := updated.Annotations[testBlockedKey]; left && !tc.wantForeign {
				t.Fatal("blocked mark must go with the adoption or the overwrite")
			}
		})
	}
}

// The cordon write is conditional on the node the manager just read. A
// Conflict re-reads: here the node was cordoned by somebody else in between,
// so the retry must find it foreign instead of stamping a claim on it.
func TestCordonManagerCordonRetriesOnConflictFromAFreshRead(t *testing.T) {
	node := cordonTestNode(false, UpgradeStateCordonRequired, nil)
	cs := k8sfake.NewClientset(node)
	nodesGVR := schema.GroupVersionResource{Version: "v1", Resource: "nodes"}
	conflicted := false
	cs.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicted {
			return false, nil, nil
		}
		conflicted = true
		// Somebody cordoned the node between the read and the write.
		cordonedByHand := node.DeepCopy()
		cordonedByHand.Spec.Unschedulable = true
		if err := cs.Tracker().Update(nodesGVR, cordonedByHand, ""); err != nil {
			t.Fatalf("update tracker: %v", err)
		}
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "worker", nil)
	})

	foreign, err := NewCordonManager(cs).Cordon(context.Background(), node.DeepCopy())
	if err != nil {
		t.Fatalf("Cordon: %v", err)
	}
	if !foreign {
		t.Fatal("a cordon placed between the read and the write is not the rollout's")
	}
	if _, claimed := getTestNode(t, cs).Annotations[testClaimKey]; claimed {
		t.Fatal("the retry must not stamp a claim on somebody else's cordon")
	}
}

func TestCordonManagerUncordon(t *testing.T) {
	tests := map[string]struct {
		node                *corev1.Node
		wantUnschedulable   bool
		wantClaimLeft       bool
		wantBlockedMarkLeft bool
	}{
		"operator's cordon is lifted, claim and mark cleared in one write": {
			node: cordonTestNode(true, UpgradeStateUncordonRequired, map[string]string{testClaimKey: consts.OperatorCordonClaimValue, testBlockedKey: "PDB"}),
		},
		"k8s-driver-manager's cordon is adopted and lifted": {
			node: cordonTestNode(true, UpgradeStateDone, map[string]string{testClaimKey: "driver", testBlockedKey: "PDB"}),
		},
		"legacy build's own cordon is lifted": {
			node: cordonTestNode(true, UpgradeStatePodRestartRequired, nil),
		},
		"administrator's cordon is left alone": {
			node:              cordonTestNode(true, UpgradeStatePodRestartRequired, map[string]string{UpgradeInitialStateAnnotationKey: trueString}),
			wantUnschedulable: true,
		},
		"unclaimed cordon on a done node is left alone": {
			node:              cordonTestNode(true, UpgradeStateDone, nil),
			wantUnschedulable: true,
		},
		// The administrator uncordoned by hand mid-rollout; the claim still
		// has to go or the next administrator's cordon reads as the rollout's.
		"stale claim on a schedulable node is cleared": {
			node: cordonTestNode(false, UpgradeStateUncordonRequired, map[string]string{testClaimKey: consts.OperatorCordonClaimValue}),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cs := k8sfake.NewClientset(tc.node)
			m := NewCordonManager(cs)
			if err := m.Uncordon(context.Background(), tc.node.DeepCopy()); err != nil {
				t.Fatalf("Uncordon: %v", err)
			}
			updated := getTestNode(t, cs)
			if updated.Spec.Unschedulable != tc.wantUnschedulable {
				t.Fatalf("unschedulable = %v, want %v", updated.Spec.Unschedulable, tc.wantUnschedulable)
			}
			if _, left := updated.Annotations[testClaimKey]; left != tc.wantClaimLeft {
				t.Fatalf("claim left = %v, want %v", left, tc.wantClaimLeft)
			}
			if _, left := updated.Annotations[testBlockedKey]; left != tc.wantBlockedMarkLeft {
				t.Fatalf("blocked mark left = %v, want %v", left, tc.wantBlockedMarkLeft)
			}
		})
	}
}

// Guards the one-write property: cordon and claim, or uncordon and release,
// must never be split across two patches.
func TestCordonPatchesCarryClaimAndState(t *testing.T) {
	var cordon map[string]any
	if err := json.Unmarshal(cordonPatch("7"), &cordon); err != nil {
		t.Fatalf("cordon patch is not JSON: %v", err)
	}
	if cordon["spec"].(map[string]any)["unschedulable"] != true {
		t.Fatal("cordon patch must set unschedulable")
	}
	meta := cordon["metadata"].(map[string]any)
	if meta["resourceVersion"] != "7" {
		t.Fatal("cordon patch must be conditional on the node that was read")
	}
	if meta["annotations"].(map[string]any)[testClaimKey] != consts.OperatorCordonClaimValue {
		t.Fatal("cordon patch must carry the claim")
	}

	var uncordon map[string]any
	if err := json.Unmarshal(uncordonPatch, &uncordon); err != nil {
		t.Fatalf("uncordon patch is not JSON: %v", err)
	}
	annotations := uncordon["metadata"].(map[string]any)["annotations"].(map[string]any)
	for _, key := range []string{testClaimKey, testBlockedKey} {
		if v, ok := annotations[key]; !ok || v != nil {
			t.Fatalf("uncordon patch must null %s", key)
		}
	}
	if uncordon["spec"].(map[string]any)["unschedulable"] != false {
		t.Fatal("uncordon patch must clear unschedulable")
	}
}
