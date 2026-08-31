package upgrade

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newDrainTestManager(t *testing.T, node *corev1.Node, reactors ...func(*k8sfake.Clientset)) (*DrainManager, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme)
	clientset := k8sfake.NewClientset()
	if node != nil {
		builder = builder.WithObjects(node)
		if _, err := clientset.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed clientset node: %v", err)
		}
	}
	for _, r := range reactors {
		r(clientset)
	}
	rec := record.NewFakeRecorder(16)
	provider := NewNodeUpgradeStateProvider(builder.Build(), rec)
	return NewDrainManager(clientset, provider, rec), rec
}

func drainNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "drain-worker",
		Labels: map[string]string{UpgradeStateLabelKey: UpgradeStateDrainRequired},
	}}
}

func enabledDrainSpec() *v1beta1.DrainSpec {
	return &v1beta1.DrainSpec{Enable: true, TimeoutSeconds: 5}
}

func waitDrainFinished(t *testing.T, m *DrainManager, nodeName string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for m.drainingNodes.Has(nodeName) {
		if time.Now().After(deadline) {
			t.Fatal("drain goroutine did not finish in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestScheduleNodesDrainSuccess(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node)

	err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{
		Spec: enabledDrainSpec(), Nodes: []*corev1.Node{node},
	})
	if err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	waitDrainFinished(t, m, node.Name)

	expectEvent(t, rec, corev1.EventTypeNormal, consts.RBLNEventReasonNodeDrained, "drained")
	if got := node.Labels[UpgradeStateLabelKey]; got != UpgradeStatePodRestartRequired {
		t.Fatalf("final state = %q, want %q", got, UpgradeStatePodRestartRequired)
	}
	expectNoEvent(t, rec)
}

func TestScheduleNodesDrainCordonFailure(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node, func(cs *k8sfake.Clientset) {
		cs.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("cordon rejected by test")
		})
	})

	err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{
		Spec: enabledDrainSpec(), Nodes: []*corev1.Node{node},
	})
	if err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	waitDrainFinished(t, m, node.Name)

	expectEvent(t, rec, corev1.EventTypeWarning, consts.RBLNEventReasonNodeDrainFailed, "cordon")
	expectNoEvent(t, rec)
	if got := node.Labels[UpgradeStateLabelKey]; got != UpgradeStateDrainRequired {
		t.Fatalf("state = %q, want unchanged %q", got, UpgradeStateDrainRequired)
	}
}

func TestScheduleNodesDrainDrainFailure(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node, func(cs *k8sfake.Clientset) {
		// Cordon succeeds; the drain phase fails when listing pods to evict.
		cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("pod list rejected by test")
		})
	})

	err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{
		Spec: enabledDrainSpec(), Nodes: []*corev1.Node{node},
	})
	if err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	waitDrainFinished(t, m, node.Name)

	expectEvent(t, rec, corev1.EventTypeWarning, consts.RBLNEventReasonNodeDrainFailed, "drain")
	expectEvent(t, rec, corev1.EventTypeWarning, consts.RBLNEventReasonDriverUpgradeFailed, "")
}

func TestScheduleNodesDrainDisabled(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node)

	err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{
		Spec: &v1beta1.DrainSpec{Enable: false}, Nodes: []*corev1.Node{node},
	})
	if err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	expectNoEvent(t, rec)
}

func TestScheduleNodesDrainNilSpec(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node)

	if err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{Nodes: []*corev1.Node{node}}); err == nil {
		t.Fatal("expected error for nil drain spec")
	}
	expectNoEvent(t, rec)
}

func TestScheduleNodesDrainEmptyNodes(t *testing.T) {
	m, rec := newDrainTestManager(t, nil)

	if err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{Spec: enabledDrainSpec()}); err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	expectNoEvent(t, rec)
}

func TestScheduleNodesDrainDuplicateSkipped(t *testing.T) {
	node := drainNode()
	m, rec := newDrainTestManager(t, node)
	m.drainingNodes.Add(node.Name)
	defer m.drainingNodes.Remove(node.Name)

	err := m.ScheduleNodesDrain(context.Background(), &DrainConfiguration{
		Spec: enabledDrainSpec(), Nodes: []*corev1.Node{node},
	})
	if err != nil {
		t.Fatalf("ScheduleNodesDrain: %v", err)
	}
	expectNoEvent(t, rec)
}
