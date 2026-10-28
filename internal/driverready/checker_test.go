package driverready

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func readyPod() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "operator", UID: "same-pod-uid", Labels: map[string]string{
			"app.kubernetes.io/component": "rbln-driver", "rebellions.ai/driver-instance": "owner",
		}},
		Spec: corev1.PodSpec{NodeName: "node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: consts.DriverContainerName, Ready: true, ContainerID: "containerd://current",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}},
	}
}

func TestCheckPods(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.Pod){
		"ready": func(*corev1.Pod) {},
		"same Pod restarted and initializing": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].Ready = false
			p.Status.ContainerStatuses[0].RestartCount++
		},
		"crash with stale ready bit": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
		},
		"terminating ready Pod": func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now },
		"wrong owner":           func(p *corev1.Pod) { p.Labels["rebellions.ai/driver-instance"] = "old-owner" },
		"other node":            func(p *corev1.Pod) { p.Spec.NodeName = "other-node" },
		"smd is not the driver": func(p *corev1.Pod) { p.Labels["app.kubernetes.io/component"] = "rbln-smd" },
		"other container ready": func(p *corev1.Pod) { p.Status.ContainerStatuses[0].Name = "sidecar" },
		"no container status":   func(p *corev1.Pod) { p.Status.ContainerStatuses = nil },
		"failed Pod":            func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed },
	} {
		t.Run(name, func(t *testing.T) {
			p := readyPod()
			mutate(&p)
			err := checkPods([]corev1.Pod{p}, "node", "owner")
			if (err == nil) != (name == "ready") {
				t.Fatalf("checkPods() = %v", err)
			}
		})
	}
	t.Run("ambiguous replacement blocks", func(t *testing.T) {
		old, next := readyPod(), readyPod()
		next.Name, next.UID = "next", "next-uid"
		if err := checkPods([]corev1.Pod{old, next}, "node", "owner"); err == nil {
			t.Fatal("two active producers must not pass")
		}
		now := metav1.Now()
		old.DeletionTimestamp = &now
		if err := checkPods([]corev1.Pod{old, next}, "node", "owner"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestChecker(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		owner     string
		allowHost bool
		apiError  string
		wantReady bool
	}{
		{name: "current driver", mode: "true", owner: "owner", wantReady: true},
		{name: "unowned node", mode: "true"},
		{name: "disabled driver", mode: "false", owner: "owner"},
		{name: "host driver downstream gate", mode: consts.RBLNDeployDriverPreInstalled, allowHost: true, wantReady: true},
		{name: "host driver without manager label", mode: "true", allowHost: true, wantReady: true},
		{name: "host driver without deploy label", allowHost: true, wantReady: true},
		{name: "SMD requires container driver", mode: consts.RBLNDeployDriverPreInstalled},
		{name: "node API error", mode: "true", owner: "owner", apiError: "nodes"},
		{name: "pod API error", mode: "true", owner: "owner", apiError: "pods"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{
				consts.RBLNDeployDriverLabelKey: tc.mode, consts.RBLNDriverOwnerLabelKey: tc.owner,
			}}}
			objects := []runtime.Object{node}
			if !tc.allowHost {
				p := readyPod()
				objects = append(objects, &p)
			}
			client := fake.NewClientset(objects...)
			if tc.apiError != "" {
				client.PrependReactor("*", tc.apiError, func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("API unavailable")
				})
			}
			c := &Checker{client: client, node: "node", namespace: "operator"}
			if tc.allowHost {
				c.hostDriverValidated = func() (bool, error) { return true, nil }
			}
			if err := c.Check(context.Background()); (err == nil) != tc.wantReady {
				t.Fatalf("Check() = %v, want ready=%v", err, tc.wantReady)
			}
			for _, a := range client.Actions() {
				if a.GetResource().Resource == "pods" && a.GetNamespace() != "operator" {
					t.Fatalf("pod lookup escaped namespace: %+v", a)
				}
			}
		})
	}
}

func TestHostValidationCannotBypassManagedDriver(t *testing.T) {
	for _, tc := range []struct {
		name          string
		owner         string
		mode          string
		podState      string
		validated     bool
		validationErr bool
		apiError      string
		wantCheckHost bool
		wantReady     bool
	}{
		{name: "validated host without driver CR", mode: "true", validated: true, wantCheckHost: true, wantReady: true},
		{name: "no owner or Pod is not proof", mode: "true", wantCheckHost: true},
		{name: "label alone is not proof", mode: consts.RBLNDeployDriverPreInstalled, wantCheckHost: true},
		{name: "invalid host record", validationErr: true, wantCheckHost: true},
		{name: "owner with missing Pod", mode: "true", owner: "owner", validated: true},
		{name: "ready managed driver ignores host record", mode: "true", owner: "owner", podState: "running", validated: true, wantReady: true},
		{name: "owner with crashed container", mode: "true", owner: "owner", podState: "crashed", validated: true},
		{name: "unowned driver Pod", mode: "true", podState: "running", validated: true},
		{name: "terminating driver Pod", podState: "terminating", validated: true},
		{name: "pending driver Pod", podState: "pending", validated: true},
		{name: "driver on another node", podState: "other node", validated: true, wantCheckHost: true, wantReady: true},
		{name: "terminal Pod no longer installs", podState: "failed", validated: true, wantCheckHost: true, wantReady: true},
		{name: "node API failure", validated: true, apiError: "nodes"},
		{name: "Pod API failure with host label", mode: consts.RBLNDeployDriverPreInstalled, validated: true, apiError: "pods"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{
				consts.RBLNDeployDriverLabelKey: tc.mode, consts.RBLNDriverOwnerLabelKey: tc.owner,
			}}}
			objects := []runtime.Object{node}
			if tc.podState != "" {
				p := readyPod()
				switch tc.podState {
				case "crashed":
					p.Status.ContainerStatuses[0].Ready = false
					p.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
				case "terminating":
					now := metav1.Now()
					p.DeletionTimestamp = &now
				case "pending":
					p.Status.Phase = corev1.PodPending
				case "other node":
					p.Spec.NodeName = "other"
				case "failed":
					p.Status.Phase = corev1.PodFailed
				}
				objects = append(objects, &p)
			}
			client := fake.NewClientset(objects...)
			if tc.apiError != "" {
				client.PrependReactor("*", tc.apiError, func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("API unavailable")
				})
			}
			checkedHost := false
			c := &Checker{client: client, node: "node", namespace: "operator", hostDriverValidated: func() (bool, error) {
				checkedHost = true
				if tc.validationErr {
					return false, errors.New("invalid driver-ready record")
				}
				return tc.validated, nil
			}}
			if err := c.Check(context.Background()); (err == nil) != tc.wantReady || checkedHost != tc.wantCheckHost {
				t.Fatalf("Check() = %v, checkedHost=%v; want ready=%v checkedHost=%v", err, checkedHost, tc.wantReady, tc.wantCheckHost)
			}
		})
	}
}
