package upgrade

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// The validator DaemonSet may not have a pod on the node yet — its deploy
// label is paused while k8s-driver-manager installs, and a pause that a
// killed run left behind is only repaired by the policy controller. The node
// must not hold its cordon in validation-required forever waiting for it.
func TestValidateWithoutValidatorPodRunsTheClock(t *testing.T) {
	tests := map[string]struct {
		startedAgo time.Duration
		wantState  string
		wantClock  bool
	}{
		"first sight stamps the clock":     {wantState: UpgradeStateValidationRequired, wantClock: true},
		"within the timeout keeps waiting": {startedAgo: time.Minute, wantState: UpgradeStateValidationRequired, wantClock: true},
		"past the timeout fails the node":  {startedAgo: 2 * time.Duration(DefaultValidationTimeoutSeconds) * time.Second, wantState: UpgradeStateFailed},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			annotations := map[string]string{}
			if tc.startedAgo != 0 {
				annotations[UpgradeValidationStartTimeAnnotationKey] = strconv.FormatInt(time.Now().Add(-tc.startedAgo).Unix(), 10)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:        "n",
				Labels:      map[string]string{UpgradeStateLabelKey: UpgradeStateValidationRequired},
				Annotations: annotations,
			}}
			provider := newTestNodeUpgradeStateProvider(t)
			if err := provider.K8sClient.Create(context.Background(), node); err != nil {
				t.Fatalf("create node: %v", err)
			}
			vm := NewValidationManager(k8sfake.NewClientset(), provider, "app=validator")

			done, err := vm.Validate(context.Background(), node.DeepCopy())
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if done {
				t.Fatal("no validator pod cannot mean validation done")
			}
			updated := &corev1.Node{}
			if err := provider.K8sClient.Get(context.Background(), types.NamespacedName{Name: "n"}, updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("state = %q, want %q", got, tc.wantState)
			}
			if _, clock := updated.Annotations[UpgradeValidationStartTimeAnnotationKey]; clock != tc.wantClock {
				t.Fatalf("clock present = %v, want %v", clock, tc.wantClock)
			}
		})
	}
}
