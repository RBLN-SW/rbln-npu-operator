package driverready

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestBindingGateWaitsForManagedDriverEvenWithOldPartitionMarker(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"partition-ready", ".driver-ctr-ready"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{consts.RBLNDriverOwnerLabelKey: "owner", consts.RBLNDeployDriverLabelKey: "true"}}}
	pod := readyPod()
	pod.Status.ContainerStatuses[0].Ready = false
	cs := fake.NewClientset(node, &pod)
	checker := &Checker{client: cs, node: "node", namespace: "operator"}
	if err := checker.CheckForBinding(context.Background(), root); err == nil {
		t.Fatal("old partition marker bypassed installing driver")
	}
	pod.Status.ContainerStatuses[0].Ready = true
	if _, err := cs.CoreV1().Pods("operator").UpdateStatus(context.Background(), &pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := checker.CheckForBinding(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, ".driver-ctr-ready")); err != nil {
		t.Fatal(err)
	}
	if err := checker.CheckForBinding(context.Background(), root); err == nil {
		t.Fatal("missing installation marker accepted")
	}
}

func TestBindingGateRetainsUnmanagedVFIOPath(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	c := &Checker{client: fake.NewClientset(node), node: "node", namespace: "operator"}
	if err := c.CheckForBinding(context.Background(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
