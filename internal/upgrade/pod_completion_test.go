package upgrade

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/metrics"
)

func TestPodCompletionPublishesCommittedState(t *testing.T) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	expired := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
	for _, tc := range []struct {
		name      string
		phase     corev1.PodPhase
		timeout   int
		start     string
		wantState string
		wantClock bool
	}{
		{name: "unlimited wait", phase: corev1.PodRunning, wantState: UpgradeStateWaitForJobsRequired},
		{name: "unlimited wait ignores old deadline", phase: corev1.PodRunning, start: expired, wantState: UpgradeStateWaitForJobsRequired, wantClock: true},
		{name: "starts running clock", phase: corev1.PodRunning, timeout: 60, wantState: UpgradeStateWaitForJobsRequired, wantClock: true},
		{name: "starts pending clock", phase: corev1.PodPending, timeout: 60, wantState: UpgradeStateWaitForJobsRequired, wantClock: true},
		{name: "retains running clock", phase: corev1.PodRunning, timeout: 60, start: now, wantState: UpgradeStateWaitForJobsRequired, wantClock: true},
		{name: "elapsed timeout", phase: corev1.PodRunning, timeout: 60, start: expired, wantState: UpgradeStatePodDeletionRequired},
		{name: "succeeded job", phase: corev1.PodSucceeded, timeout: 60, start: now, wantState: UpgradeStatePodDeletionRequired},
		{name: "failed job", phase: corev1.PodFailed, timeout: 60, start: now, wantState: UpgradeStatePodDeletionRequired},
		{name: "no matching jobs", timeout: 60, start: now, wantState: UpgradeStatePodDeletionRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer metrics.DriverUpgradeNodes.Reset()
			ctx := t.Context()
			m := newTestManager(t)
			n := newNodeUpgradeState("worker", UpgradeStateWaitForJobsRequired, "rev1")
			if tc.start != "" {
				n.Node.Annotations = map[string]string{UpgradeWaitForPodCompletionStartTimeAnnotationKey: tc.start}
			}
			registerNodes(t, m, n.Node)
			job := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "jobs", Labels: map[string]string{"app": "wait"}},
				Spec:       corev1.PodSpec{NodeName: n.Node.Name},
				Status:     corev1.PodStatus{Phase: tc.phase},
			}
			// Neither an unmatched pod nor a matching pod on another node may
			// hold up this node's rollout.
			unmatched := job.DeepCopy()
			unmatched.Name, unmatched.Labels["app"], unmatched.Status.Phase = "unmatched", "other", corev1.PodRunning
			otherNode := job.DeepCopy()
			otherNode.Name, otherNode.Spec.NodeName, otherNode.Status.Phase = "other-node", "another-worker", corev1.PodRunning
			objects := []runtime.Object{unmatched, otherNode}
			if tc.phase != "" {
				objects = append(objects, job)
			}
			m.podManager = NewPodManager(k8sfake.NewClientset(objects...), m.nodeUpgradeStateProvider, nil)
			state := newClusterState(map[string][]*NodeUpgradeState{UpgradeStateWaitForJobsRequired: {n}})
			policy := &v1beta1.DriverUpgradePolicySpec{
				AutoUpgrade:       true,
				WaitForCompletion: &v1beta1.WaitForCompletionSpec{PodSelector: "app=wait", TimeoutSeconds: tc.timeout},
			}
			if err := m.ApplyState(ctx, state, policy, ""); err != nil {
				t.Fatal(err)
			}
			live, err := m.nodeUpgradeStateProvider.GetNode(ctx, n.Node.Name)
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range []*corev1.Node{live, n.Node} {
				if got := node.Labels[UpgradeStateLabelKey]; got != tc.wantState {
					t.Fatalf("live and snapshot must agree: state=%q, want %q", got, tc.wantState)
				}
				clock, present := node.Annotations[UpgradeWaitForPodCompletionStartTimeAnnotationKey]
				if present != tc.wantClock || (tc.wantClock && tc.start != "" && clock != tc.start) {
					t.Fatalf("unexpected completion clock: value=%q, present=%v", clock, present)
				}
			}
			if n.Node.Annotations[UpgradeWaitForPodCompletionStartTimeAnnotationKey] != live.Annotations[UpgradeWaitForPodCompletionStartTimeAnnotationKey] {
				t.Fatal("snapshot did not receive the persisted completion clock")
			}
			if state.nodes[0].phase != UpgradeStateWaitForJobsRequired {
				t.Fatal("a committed transition must not change this pass's phase selection")
			}
			for _, phase := range []string{UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired, UpgradeStatePodRestartRequired} {
				want := float64(0)
				if phase == tc.wantState {
					want = 1
				}
				if got := testutil.ToFloat64(metrics.DriverUpgradeNodes.WithLabelValues(phase)); got != want {
					t.Fatalf("metric %s=%v, want %v", phase, got, want)
				}
			}
		})
	}
}

func TestPodCompletionRetriesFailuresWithoutResettingDeadline(t *testing.T) {
	for _, failureAt := range []string{"list", "transition"} {
		t.Run(failureAt, func(t *testing.T) {
			ctx := t.Context()
			m := newTestManager(t)
			blocked := newNodeUpgradeState("blocked", UpgradeStateWaitForJobsRequired, "rev1")
			deadline := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
			blocked.Node.Annotations = map[string]string{UpgradeWaitForPodCompletionStartTimeAnnotationKey: deadline}
			finished := newNodeUpgradeState("finished", UpgradeStateWaitForJobsRequired, "rev1")
			unavailable := errors.New("API unavailable")
			fail := true
			c := fake.NewClientBuilder().WithScheme(m.k8sClient.Scheme()).WithObjects(blocked.Node, finished.Node).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if fail && failureAt == "transition" && obj.GetName() == blocked.Node.Name {
							return unavailable
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
				}).Build()
			provider := NewNodeUpgradeStateProvider(c, nil)
			cs := k8sfake.NewClientset(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "jobs", Labels: map[string]string{"app": "wait"}},
				Spec:       corev1.PodSpec{NodeName: blocked.Node.Name},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			})
			cs.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				selector := action.(k8stesting.ListAction).GetListRestrictions().Fields.String()
				if fail && failureAt == "list" && selector == "spec.nodeName="+blocked.Node.Name {
					return true, nil, unavailable
				}
				return false, nil, nil
			})
			pm := NewPodManager(cs, provider, nil)
			cfg := &PodManagerConfig{
				Nodes:                 []*corev1.Node{blocked.Node, finished.Node},
				WaitForCompletionSpec: &v1beta1.WaitForCompletionSpec{PodSelector: "app=wait", TimeoutSeconds: 60},
			}
			if err := pm.ScheduleCheckOnPodCompletion(ctx, cfg); !errors.Is(err, unavailable) {
				t.Fatalf("observation and write failures must be returned: %v", err)
			}
			live, err := provider.GetNode(ctx, blocked.Node.Name)
			if err != nil {
				t.Fatal(err)
			}
			if live.Labels[UpgradeStateLabelKey] != UpgradeStateWaitForJobsRequired ||
				live.Annotations[UpgradeWaitForPodCompletionStartTimeAnnotationKey] != deadline {
				t.Fatal("failed attempt changed the phase or reset the deadline")
			}
			if finished.Node.Labels[UpgradeStateLabelKey] != UpgradeStatePodDeletionRequired {
				t.Fatal("one node's failure prevented another node from completing")
			}
			fail = false
			cfg.Nodes = []*corev1.Node{blocked.Node}
			if err := pm.ScheduleCheckOnPodCompletion(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			live, err = provider.GetNode(ctx, blocked.Node.Name)
			if err != nil {
				t.Fatal(err)
			}
			if live.Labels[UpgradeStateLabelKey] != UpgradeStatePodDeletionRequired ||
				blocked.Node.Labels[UpgradeStateLabelKey] != UpgradeStatePodDeletionRequired ||
				live.Annotations[UpgradeWaitForPodCompletionStartTimeAnnotationKey] != "" {
				t.Fatal("retry did not finish the elapsed wait and clear its deadline")
			}
		})
	}
}
