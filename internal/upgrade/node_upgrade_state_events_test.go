package upgrade

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newEventTestProvider(t *testing.T, funcs interceptor.Funcs) (*NodeUpgradeStateProvider, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(funcs).Build()
	rec := record.NewFakeRecorder(16)
	return NewNodeUpgradeStateProvider(k8sClient, logr.Discard(), rec), rec
}

func createStateNode(t *testing.T, p *NodeUpgradeStateProvider, state string) *corev1.Node {
	t.Helper()
	labels := map[string]string{}
	if state != "" {
		labels[UpgradeStateLabelKey] = state
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Labels: labels}}
	if err := p.K8sClient.Create(context.Background(), node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	return node
}

func TestChangeNodeUpgradeStateEventTable(t *testing.T) {
	tests := []struct {
		name       string
		oldState   string
		newState   string
		wantType   string
		wantReason string
	}{
		{"started", UpgradeStateUpgradeRequired, UpgradeStateCordonRequired, corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeStarted},
		{"completed from in-progress", UpgradeStateUncordonRequired, UpgradeStateDone, corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeCompleted},
		{"failed from drain", UpgradeStateDrainRequired, UpgradeStateFailed, corev1.EventTypeWarning, consts.RBLNEventReasonDriverUpgradeFailed},
		{"initial unknown to done", "", UpgradeStateDone, "", ""},
		{"uncontracted transition", UpgradeStateWaitForJobsRequired, UpgradeStateDrainRequired, "", ""},
		{"failed recovery counts as completed", UpgradeStateFailed, UpgradeStateDone, corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeCompleted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, rec := newEventTestProvider(t, interceptor.Funcs{})
			node := createStateNode(t, p, tc.oldState)

			if err := p.ChangeNodeUpgradeState(context.Background(), node, tc.newState); err != nil {
				t.Fatalf("ChangeNodeUpgradeState: %v", err)
			}
			if node.Labels[UpgradeStateLabelKey] != tc.newState {
				t.Fatalf("caller node label not refreshed: %q", node.Labels[UpgradeStateLabelKey])
			}
			if tc.wantReason == "" {
				expectNoEvent(t, rec)
				return
			}
			expectEvent(t, rec, tc.wantType, tc.wantReason, "")
		})
	}
}

func TestChangeNodeUpgradeStateSameStateIsNoOp(t *testing.T) {
	patches := 0
	p, rec := newEventTestProvider(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	node := createStateNode(t, p, UpgradeStateCordonRequired)

	if err := p.ChangeNodeUpgradeState(context.Background(), node, UpgradeStateCordonRequired); err != nil {
		t.Fatalf("ChangeNodeUpgradeState: %v", err)
	}
	if patches != 0 {
		t.Fatalf("expected no patch calls on same-state reset, got %d", patches)
	}
	expectNoEvent(t, rec)
}

func TestChangeNodeUpgradeStatePatchFailureEmitsNothing(t *testing.T) {
	p, rec := newEventTestProvider(t, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return context.DeadlineExceeded
		},
	})
	node := createStateNode(t, p, UpgradeStateUpgradeRequired)

	if err := p.ChangeNodeUpgradeState(context.Background(), node, UpgradeStateCordonRequired); err == nil {
		t.Fatal("expected error from failing patch")
	}
	expectNoEvent(t, rec)
	if node.Labels[UpgradeStateLabelKey] != UpgradeStateUpgradeRequired {
		t.Fatalf("caller label mutated on failure: %q", node.Labels[UpgradeStateLabelKey])
	}
}

// The involvedObject of every upgrade event must be the Node itself.
func TestChangeNodeUpgradeStateEventObjectIsNode(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	spy := &spyRecorder{}
	p := NewNodeUpgradeStateProvider(fake.NewClientBuilder().WithScheme(scheme).Build(), logr.Discard(), spy)
	node := createStateNode(t, p, UpgradeStateUpgradeRequired)

	if err := p.ChangeNodeUpgradeState(context.Background(), node, UpgradeStateCordonRequired); err != nil {
		t.Fatalf("ChangeNodeUpgradeState: %v", err)
	}
	if len(spy.objects) != 1 {
		t.Fatalf("events = %d, want 1", len(spy.objects))
	}
	emitted, ok := spy.objects[0].(*corev1.Node)
	if !ok {
		t.Fatalf("involvedObject type = %T, want *corev1.Node", spy.objects[0])
	}
	if emitted.Name != node.Name {
		t.Fatalf("involvedObject name = %q, want %q", emitted.Name, node.Name)
	}
	if spy.reasons[0] != consts.RBLNEventReasonDriverUpgradeStarted {
		t.Fatalf("reason = %q", spy.reasons[0])
	}
}

func TestChangeNodeUpgradeStateNilRecorder(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	p := NewNodeUpgradeStateProvider(fake.NewClientBuilder().WithScheme(scheme).Build(), logr.Discard(), nil)
	node := createStateNode(t, p, UpgradeStateUpgradeRequired)

	if err := p.ChangeNodeUpgradeState(context.Background(), node, UpgradeStateCordonRequired); err != nil {
		t.Fatalf("nil recorder must not fail transitions: %v", err)
	}
}
