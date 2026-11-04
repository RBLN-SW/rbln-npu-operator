package upgrade

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
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

			done, err := vm.Validate(context.Background(), node.DeepCopy(), ValidationOptions{})
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

func TestValidationStillUnblocksSafeLoad(t *testing.T) {
	m := newTestManager(t, withValidationEnabled(), withValidationManager(&mockValidationManager{done: false}))
	n := newNodeUpgradeState("worker", UpgradeStateValidationRequired, "rev1")
	n.Node.Annotations = map[string]string{UpgradeWaitForSafeDriverLoadAnnotationKey: "true"}
	registerNodes(t, m, n.Node)
	if err := m.ProcessValidationRequiredNodes(context.Background(), newClusterState(map[string][]*NodeUpgradeState{UpgradeStateValidationRequired: {n}}), ValidationOptions{}); err != nil {
		t.Fatal(err)
	}
	stored, err := m.nodeUpgradeStateProvider.GetNode(context.Background(), n.Node.Name)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != "" {
		t.Fatal("validation leaves the driver blocked on safe-load gate")
	}
}

func TestValidationRequiresEveryMatchingPodToBeReady(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ready []bool
		want  bool
	}{
		{name: "no validator"},
		{name: "one ready", ready: []bool{true}, want: true},
		{name: "one unready", ready: []bool{false}},
		{name: "ready before unready", ready: []bool{true, false}},
		{name: "unready before ready", ready: []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestNodeUpgradeStateProvider(t)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Labels: map[string]string{UpgradeStateLabelKey: UpgradeStateValidationRequired}}}
			if err := provider.K8sClient.Create(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			var objects []runtime.Object
			for index, ready := range tc.ready {
				objects = append(objects, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("validator-%d", index), Namespace: "operator", Labels: map[string]string{"app": "validator"}},
					Spec:       corev1.PodSpec{NodeName: node.Name}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Ready: ready}}},
				})
			}
			manager := NewValidationManager(k8sfake.NewClientset(objects...), provider, "app=validator")
			done, err := manager.Validate(context.Background(), node, ValidationOptions{})
			if err != nil || done != tc.want {
				t.Fatalf("validation=%v err=%v, want %v", done, err, tc.want)
			}
		})
	}
}

func TestValidationUsesTheConfiguredWorkloadConsumer(t *testing.T) {
	for _, tc := range []struct {
		name, workload, fallback, component, namespace  string
		dra, sandbox, ready, terminating, want, wantErr bool
	}{
		{name: "PF VM sandbox", workload: "vm-passthrough", sandbox: true, component: "sandbox-device-plugin", ready: true, want: true},
		{name: "VF VM DRA", workload: "vm-passthrough", dra: true, component: "dra-kubelet-plugin", ready: true, want: true},
		{name: "VM default without a node label", fallback: "vm-passthrough", dra: true, component: "dra-kubelet-plugin", ready: true, want: true},
		{name: "invalid label uses policy default", workload: "invalid", fallback: "vm-passthrough", sandbox: true, component: "sandbox-device-plugin", ready: true, want: true},
		{name: "container label overrides VM default", workload: "container", fallback: "vm-passthrough", dra: true, component: "operator-validator", ready: true, want: true},
		{name: "VM label overrides container default", workload: "vm-passthrough", fallback: "container", sandbox: true, component: "sandbox-device-plugin", ready: true, want: true},
		{name: "native SRIOV uses container validator", workload: "container", component: "operator-validator", ready: true, want: true},
		{name: "VM does not accept container validator", workload: "vm-passthrough", dra: true, component: "operator-validator", ready: true},
		{name: "wrong VM plugin", workload: "vm-passthrough", dra: true, component: "sandbox-device-plugin", ready: true},
		{name: "VM plugin still initializing", workload: "vm-passthrough", dra: true, component: "dra-kubelet-plugin"},
		{name: "terminating VM plugin", workload: "vm-passthrough", sandbox: true, component: "sandbox-device-plugin", ready: true, terminating: true},
		{name: "another operator namespace", workload: "vm-passthrough", dra: true, component: "dra-kubelet-plugin", namespace: "other", ready: true},
		{name: "no VM plugin configured", workload: "vm-passthrough", wantErr: true},
		{name: "both VM plugins configured", workload: "vm-passthrough", dra: true, sandbox: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := newTestNodeUpgradeStateProvider(t)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{UpgradeStateLabelKey: UpgradeStateValidationRequired}}}
			if tc.workload != "" {
				node.Labels[consts.RBLNWorkloadConfigLabelKey] = tc.workload
			}
			if err := provider.K8sClient.Create(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			var pods []runtime.Object
			if tc.component != "" {
				namespace := tc.namespace
				if namespace == "" {
					namespace = "operator"
				}
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/name": consts.OperatorName, "app.kubernetes.io/component": tc.component}},
					Spec:       corev1.PodSpec{NodeName: node.Name},
					Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: tc.component, Ready: tc.ready}}},
				}
				if tc.terminating {
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				pods = append(pods, pod)
			}
			manager := NewValidationManager(k8sfake.NewClientset(pods...), provider, "app.kubernetes.io/name=rbln-npu-operator,app.kubernetes.io/component=operator-validator")
			done, err := manager.Validate(context.Background(), node, ValidationOptions{Namespace: "operator", DefaultWorkload: tc.fallback, DRAEnabled: tc.dra, SandboxEnabled: tc.sandbox})
			if done != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("done=%v err=%v, want done=%v err=%v", done, err, tc.want, tc.wantErr)
			}
		})
	}
}
