package upgrade

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func blockTestPod(name string, owner *metav1.OwnerReference, emptyDir bool) corev1.Pod {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "workloads", Name: name}}
	if owner != nil {
		pod.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	if emptyDir {
		pod.Spec.Volumes = []corev1.Volume{{
			Name:         "shm",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}
	}
	return pod
}

func controllerRef(kind, name string) *metav1.OwnerReference {
	isController := true
	return &metav1.OwnerReference{APIVersion: "apps/v1", Kind: kind, Name: name, UID: types.UID("uid-" + name), Controller: &isController}
}

func TestEvictionBlockReason(t *testing.T) {
	rs := controllerRef("ReplicaSet", "rs")
	ds := controllerRef("DaemonSet", "npu-ds")

	tests := map[string]struct {
		blocked    []corev1.Pod
		spec       v1beta1.PodDeletionSpec
		helperErrs []error
		want       []string
		wantAbsent []string
		wantOrder  []string
	}{
		"emptyDir pods are listed sorted with the knob to flip": {
			blocked: []corev1.Pod{blockTestPod("vllm-b", rs, true), blockTestPod("vllm-a", rs, true)},
			want: []string{
				"2 NPU pod(s) use emptyDir volumes: workloads/vllm-a, workloads/vllm-b",
				"podDeletion.deleteEmptyDirData=true",
			},
			wantAbsent: []string{"--delete-emptydir-data"},
		},
		"controller-less pods point at force": {
			blocked: []corev1.Pod{blockTestPod("bare", nil, false)},
			want:    []string{"1 NPU pod(s) declare no controller: workloads/bare", "podDeletion.force=true"},
		},
		"DaemonSet pods explain why no knob helps": {
			blocked: []corev1.Pod{blockTestPod("ds-npu", ds, false)},
			want:    []string{"DaemonSet-managed and cannot be evicted: workloads/ds-npu", "exclude the node from that DaemonSet"},
		},
		"knobs already on move the pod to the next explanation": {
			blocked: []corev1.Pod{blockTestPod("bare-shm", nil, true)},
			spec:    v1beta1.PodDeletionSpec{DeleteEmptyDirData: true},
			want:    []string{"declare no controller: workloads/bare-shm"},
			wantAbsent: []string{
				"emptyDir",
			},
		},
		"unexplained pods carry the helper errors": {
			blocked:    []corev1.Pod{blockTestPod("mystery", rs, false)},
			helperErrs: []error{errors.New("cannot delete mirror pods")},
			want:       []string{"1 NPU pod(s) were not evictable: workloads/mystery", "drain helper: cannot delete mirror pods"},
		},
		"explained pods do not drag in helper errors about other pods": {
			blocked:    []corev1.Pod{blockTestPod("vllm", rs, true)},
			helperErrs: []error{errors.New("Pods with local storage: infra/prometheus-0")},
			want:       []string{"workloads/vllm"},
			wantAbsent: []string{"prometheus"},
		},
		"kinds are joined in a fixed order": {
			blocked:   []corev1.Pod{blockTestPod("ds-npu", ds, false), blockTestPod("bare", nil, false), blockTestPod("vllm", rs, true)},
			wantOrder: []string{"use emptyDir volumes: workloads/vllm", "declare no controller: workloads/bare", "DaemonSet-managed and cannot be evicted: workloads/ds-npu"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			spec := tc.spec
			got := evictionBlockReason(tc.blocked, &spec, tc.helperErrs)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("reason %q missing %q", got, want)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(got, absent) {
					t.Fatalf("reason %q must not mention %q", got, absent)
				}
			}
			last := -1
			for _, clause := range tc.wantOrder {
				idx := strings.Index(got, clause)
				if idx <= last {
					t.Fatalf("reason %q: clause %q out of order or missing", got, clause)
				}
				last = idx
			}
		})
	}
}

// markNodeUpgradeSkipped stamps the reason into a node annotation after
// truncating it at 400 characters. The pod list is the only unbounded part of
// the message and sits ahead of the remedy, so without a cap a busy node
// produces a skip reason that names no fix.
func TestEvictionBlockReasonKeepsRemedyUnderTruncation(t *testing.T) {
	rs := controllerRef("ReplicaSet", "rs")
	blocked := make([]corev1.Pod, 0, 12)
	for i := 0; i < 12; i++ {
		blocked = append(blocked, blockTestPod(fmt.Sprintf("vllm-llama3-70b-7d9f8b6c5-ab%dde", i), rs, true))
	}

	spec := v1beta1.PodDeletionSpec{}
	reason := "pod eviction blocked: " + evictionBlockReason(blocked, &spec, nil)

	if got := truncateReason(reason); got != reason {
		t.Fatalf("reason of %d characters is truncated to %q", len(reason), got)
	}
	for _, want := range []string{"12 NPU pod(s)", "and 9 more", "deleteEmptyDirData=true"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("reason %q missing %q", reason, want)
		}
	}
}

func TestBlockedNPUPods(t *testing.T) {
	rs := controllerRef("ReplicaSet", "rs")
	a, b, c := blockTestPod("a", rs, false), blockTestPod("b", rs, false), blockTestPod("c", rs, false)

	blocked := blockedNPUPods([]corev1.Pod{a, b, c}, []corev1.Pod{b})

	if len(blocked) != 2 || blocked[0].Name != "a" || blocked[1].Name != "c" {
		names := make([]string, 0, len(blocked))
		for i := range blocked {
			names = append(names, blocked[i].Name)
		}
		t.Fatalf("blocked = %v, want [a c]", names)
	}
	if got := blockedNPUPods([]corev1.Pod{a}, []corev1.Pod{a}); len(got) != 0 {
		t.Fatalf("fully deletable set must yield no blocked pods, got %d", len(got))
	}
}
