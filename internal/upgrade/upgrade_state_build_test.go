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

	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/metrics"
)

func TestPendingDriverPodDoesNotBlockCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Labels: map[string]string{consts.RBLNDeployDriverLabelKey: "true"}}}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "operator", UID: "driver", Labels: map[string]string{"app": "driver"}}, Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 2}}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "operator", Labels: map[string]string{"app": "driver"}, OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", APIVersion: "apps/v1", Name: ds.Name, UID: ds.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: node.Name}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	pending := pod.DeepCopy()
	pending.Name = "pending"
	pending.Spec.NodeName = ""
	pending.Status.Phase = corev1.PodPending
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, ds, pod, pending).Build()
	m := newTestManager(t)
	m.k8sClient = c
	m.nodeUpgradeStateProvider = NewNodeUpgradeStateProvider(c, nil)
	state, err := m.BuildState(context.Background(), "operator", map[string]string{"app": "driver"})
	if err != nil {
		t.Fatalf("Pending pod must be ignored without blocking other nodes: %v", err)
	}
	if len(state.nodesAt(UpgradeStateUnknown)) != 1 {
		t.Fatalf("expected running node to remain visible: %+v", state)
	}
}

func TestSnapshotStatusAndMetricsExcludeLingeringDepartedPods(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	active := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "active", Labels: map[string]string{
		consts.RBLNDeployDriverLabelKey: trueString, UpgradeStateLabelKey: UpgradeStateUpgradeRequired,
	}}}
	departed := departedTestNode("departed", UpgradeStatePodRestartRequired, "false", map[string]string{
		consts.DriverManagerCordonClaimAnnotation: consts.OperatorCordonClaimValue,
	})
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "operator", UID: "set", Labels: map[string]string{"app": "driver"}},
		Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 2},
	}
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "active-pod", Namespace: ds.Namespace, Labels: map[string]string{"app": "driver"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", APIVersion: "apps/v1", Name: ds.Name, UID: ds.UID, Controller: &controller}},
		},
		Spec: corev1.PodSpec{NodeName: active.Name},
	}
	lingering := pod.DeepCopy()
	lingering.Name, lingering.Spec.NodeName = "departed-pod", departed.Name
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(active, departed, ds, pod, lingering).Build()
	m := newTestManager(t)
	m.k8sClient = c
	m.nodeUpgradeStateProvider = NewNodeUpgradeStateProvider(c, nil)
	state, err := m.BuildState(ctx, ds.Namespace, map[string]string{"app": "driver"})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.nodes) != 1 || len(state.DepartedNodes) != 1 {
		t.Fatalf("active nodes=%d, departed nodes=%d", len(state.nodes), len(state.DepartedNodes))
	}
	if err := m.ApplyState(ctx, state, &v1beta1.DriverUpgradePolicySpec{AutoUpgrade: true, MaxParallelUpgrades: 1}, "npu.example.com"); err != nil {
		t.Fatal(err)
	}
	summary := SummarizeClusterUpgrade(state)
	if summary.Total != 1 || summary.InProgress != 1 || summary.Pending != 0 {
		t.Fatalf("status counted a departed node or stale phase: %+v", summary)
	}
	for label, want := range map[string]float64{UpgradeStateUpgradeRequired: 0, UpgradeStateCordonRequired: 1, UpgradeStatePodRestartRequired: 0} {
		if got := testutil.ToFloat64(metrics.DriverUpgradeNodes.WithLabelValues(label)); got != want {
			t.Fatalf("metric %q=%v, want %v", label, got, want)
		}
	}
	// Admission changed the live label, but must not run the cordon phase in
	// the same pass or restart the departed pod.
	if len(m.cordonManager.(*mockCordonManager).cordonedNodes) != 0 || len(m.podManager.(*mockPodManager).restartedPods) != 0 {
		t.Fatal("a node advanced twice or a departed pod was restarted")
	}
	// Bookkeeping is now gone; the old DaemonSet pod still exists. It must
	// remain outside the active set on the next observation as well.
	next, err := m.BuildState(ctx, ds.Namespace, map[string]string{"app": "driver"})
	if err != nil {
		t.Fatal(err)
	}
	if got := SummarizeClusterUpgrade(next).Total; got != 1 {
		t.Fatalf("departed pod reentered the next snapshot: total=%d", got)
	}
}

func TestBuildStateObservations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"stable", 1, false},
		{"missing-pod", 0, false},
		{"unobserved", 1, false},
		{"deleting-daemonset", 1, false},
		{"unscheduled", 0, false},
		{"missing-node-name-nonpending", 0, true},
		{"duplicate", 0, false},
		{"unknown-owner", 0, false},
		{"unknown-state", 1, false},
		{"missing-node", 0, true},
		{"disabled-node", 0, false},
		{"terminating-pod", 1, false},
		{"unowned-pod", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := appsv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Labels: map[string]string{consts.RBLNDeployDriverLabelKey: trueString}}}
			set := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "operator", UID: "driver-set", Generation: 1, Labels: map[string]string{"app": "driver"}}, Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, ObservedGeneration: 1}}
			controller := true
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "driver-pod", Namespace: "operator", UID: "driver-pod", Labels: map[string]string{"app": "driver"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: set.Name, UID: set.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: node.Name}}
			switch tc.name {
			case "missing-pod":
				set.Status.DesiredNumberScheduled = 2
			case "unobserved":
				set.Status.ObservedGeneration = 0
			case "deleting-daemonset":
				now := metav1.Now()
				set.DeletionTimestamp = &now
				set.Finalizers = []string{"test"}
			case "unscheduled":
				pod.Spec.NodeName = ""
				pod.Status.Phase = corev1.PodPending
			case "missing-node-name-nonpending":
				pod.Spec.NodeName = ""
			case "unknown-owner":
				pod.OwnerReferences[0].UID = "deleted-driver-set"
			case "unknown-state":
				node.Labels[UpgradeStateLabelKey] = "unrecognized"
			case "disabled-node":
				node.Labels[consts.RBLNDeployDriverLabelKey] = "false"
			case "terminating-pod":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				pod.Finalizers = []string{"test"}
			case "unowned-pod":
				pod.OwnerReferences = nil
			}
			objects := []runtime.Object{set, pod}
			if tc.name != "missing-node" {
				objects = append(objects, node)
			}
			if tc.name == "duplicate" {
				other := pod.DeepCopy()
				other.Name = "overlap"
				other.UID = "overlap"
				objects = append(objects, other)
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
			manager := newTestManager(t)
			manager.k8sClient = client
			manager.k8sInterface = kfake.NewClientset(objects...)
			manager.nodeUpgradeStateProvider = NewNodeUpgradeStateProvider(client, nil)
			state, err := manager.BuildState(context.Background(), "operator", map[string]string{"app": "driver"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("BuildState error = %v, want error=%v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			count := len(state.nodes)
			for _, observed := range state.nodes {
				if observed.phase != UpgradeStateUnknown {
					t.Errorf("unexpected state %q", observed.phase)
				}
			}
			if count != tc.count {
				t.Fatalf("nodes = %d, want %d", count, tc.count)
			}
		})
	}
}
