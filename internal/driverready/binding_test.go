package driverready

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	driverapi "github.com/RBLN-SW/rbln-npu-operator/api/v1alpha1"
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

func TestBindingGateDistinguishesConfiguredDriverFromDeployEligibility(t *testing.T) {
	now := metav1.Now()
	for _, tc := range []struct {
		name      string
		drivers   []runtime.Object
		denyList  bool
		wantReady bool
	}{
		{name: "unmanaged VM with operator deploy label", wantReady: true},
		{name: "matching driver awaiting owner", drivers: []runtime.Object{driverConfig("driver", map[string]string{"pool": "vm"})}},
		{name: "driver for another pool", drivers: []runtime.Object{driverConfig("driver", map[string]string{"pool": "other"})}, wantReady: true},
		{name: "fallback driver awaiting owner", drivers: []runtime.Object{driverConfig("driver", nil)}},
		{name: "conflicting candidates still require assignment", drivers: []runtime.Object{driverConfig("one", nil), driverConfig("two", nil)}},
		{name: "unroutable reserved selector", drivers: []runtime.Object{driverConfig("driver", map[string]string{consts.RBLNDeployDriverLabelKey: "true"})}, wantReady: true},
		{name: "unroutable owner name", drivers: []runtime.Object{driverConfig(strings.Repeat("x", 64), nil)}, wantReady: true},
		{name: "deleting driver", drivers: []runtime.Object{&driverapi.RBLNDriver{ObjectMeta: metav1.ObjectMeta{Name: "old", DeletionTimestamp: &now}}}, wantReady: true},
		{name: "unreadable driver configuration", denyList: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method != http.MethodGet || r.URL.Path != "/apis/rebellions.ai/v1alpha1/rblndrivers" {
					http.NotFound(w, r)
					return
				}
				if tc.denyList {
					w.WriteHeader(http.StatusForbidden)
					_ = json.NewEncoder(w).Encode(metav1.Status{Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403})
					return
				}
				list := driverapi.RBLNDriverList{TypeMeta: metav1.TypeMeta{APIVersion: driverapi.GroupVersion.String(), Kind: "RBLNDriverList"}}
				for _, object := range tc.drivers {
					list.Items = append(list.Items, *object.(*driverapi.RBLNDriver))
				}
				_ = json.NewEncoder(w).Encode(list)
			}))
			defer server.Close()
			drivers, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{consts.RBLNDeployDriverLabelKey: "true", "pool": "vm"}}}
			c := &Checker{client: fake.NewClientset(node), drivers: drivers, node: "node", namespace: "operator"}
			err = c.CheckForBinding(context.Background(), t.TempDir())
			if (err == nil) != tc.wantReady {
				t.Fatalf("ready=%v err=%v", tc.wantReady, err)
			}
		})
	}
}

func driverConfig(name string, selector map[string]string) *driverapi.RBLNDriver {
	return &driverapi.RBLNDriver{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: driverapi.RBLNDriverSpec{NodeSelector: selector}}
}
