package upgrade

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func TestGetPodControllerRevisionHash(t *testing.T) {
	pm := &PodManager{}

	t.Run("returns hash when label exists", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{PodControllerRevisionHashLabelKey: "abc123"},
			},
		}
		hash, err := pm.GetPodControllerRevisionHash(pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != "abc123" {
			t.Fatalf("hash = %q, want %q", hash, "abc123")
		}
	})

	t.Run("returns error when label is missing", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}
		_, err := pm.GetPodControllerRevisionHash(pod)
		if err == nil {
			t.Fatal("expected error for missing label")
		}
	})
}

func TestIsPodRunningOrPending(t *testing.T) {
	pm := &PodManager{}

	tests := map[string]struct {
		phase corev1.PodPhase
		want  bool
	}{
		"Running":   {phase: corev1.PodRunning, want: true},
		"Pending":   {phase: corev1.PodPending, want: true},
		"Succeeded": {phase: corev1.PodSucceeded, want: false},
		"Failed":    {phase: corev1.PodFailed, want: false},
		"Unknown":   {phase: corev1.PodUnknown, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := corev1.Pod{Status: corev1.PodStatus{Phase: tc.phase}}
			if got := pm.IsPodRunningOrPending(t.Context(), pod); got != tc.want {
				t.Fatalf("IsPodRunningOrPending(%s) = %t, want %t", tc.phase, got, tc.want)
			}
		})
	}
}

func TestMarkNodeUpgradeSkippedAsync(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "evict-worker",
		Labels: map[string]string{UpgradeStateLabelKey: UpgradeStatePodDeletionRequired},
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	pm := &PodManager{nodeUpgradeStateProvider: NewNodeUpgradeStateProvider(k8sClient, nil)}

	pm.markNodeUpgradeSkippedAsync(context.Background(), *node, "pod eviction failed: PDB webapp-pdb")

	updated := &corev1.Node{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: node.Name}, updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateSkipped {
		t.Fatalf("state = %q, want %q", got, UpgradeStateSkipped)
	}
	if reason := updated.Annotations[UpgradeSkipReasonAnnotationKey]; !strings.Contains(reason, "PDB webapp-pdb") {
		t.Fatalf("skip reason = %q, want it to carry the eviction failure", reason)
	}
}

func waitEvictionFinished(t *testing.T, pm *PodManager, nodeName string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for pm.nodesInProgress.Has(nodeName) {
		if time.Now().After(deadline) {
			t.Fatal("eviction goroutine did not finish in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The eviction runs through the real kubectl drain helper against a fake
// clientset, so these cases exercise the helper's filter order rather than a
// mock of it.
func TestSchedulePodEviction(t *testing.T) {
	const nodeName = "evict-worker"
	const ns = "workloads"
	isController := true
	rsOwner := &metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid", Controller: &isController}
	dsOwner := &metav1.OwnerReference{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "npu-ds", UID: "ds-uid", Controller: &isController}
	npuDS := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "npu-ds"}}

	npuFilter := func(pod corev1.Pod) bool {
		if pod.Status.Phase != corev1.PodRunning {
			return false
		}
		for _, c := range pod.Spec.Containers {
			if _, ok := c.Resources.Limits["rebellions.ai/npu"]; ok {
				return true
			}
		}
		return false
	}
	newPod := func(name string, npu, emptyDir bool, owner *metav1.OwnerReference) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: corev1.PodSpec{
				NodeName:   nodeName,
				Containers: []corev1.Container{{Name: "main", Image: "img"}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if npu {
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"rebellions.ai/npu": resource.MustParse("1")}
		}
		if emptyDir {
			pod.Spec.Volumes = []corev1.Volume{{
				Name:         "shm",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}}
		}
		if owner != nil {
			pod.OwnerReferences = []metav1.OwnerReference{*owner}
		}
		return pod
	}
	// A DRA pod holds its NPU through a ResourceClaim and carries no
	// rebellions.ai/* resource request at all.
	withClaim := func(pod *corev1.Pod, claimName string) *corev1.Pod {
		pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "npu", ResourceClaimName: &claimName}}
		return pod
	}

	tests := map[string]struct {
		spec        v1beta1.PodDeletionSpec
		pods        []*corev1.Pod
		draObjs     []runtime.Object
		wantState   string
		wantReason  []string
		wantAbsent  []string
		wantDeleted []string
		wantKept    []string
	}{
		"emptyDir NPU pod parks the node and is named in the reason": {
			spec:       v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:       []*corev1.Pod{newPod("vllm", true, true, rsOwner), newPod("sidecar", false, true, rsOwner)},
			wantState:  UpgradeStateSkipped,
			wantReason: []string{"use emptyDir volumes", ns + "/vllm", "deleteEmptyDirData=true"},
			wantAbsent: []string{"sidecar", "--delete-emptydir-data"},
			wantKept:   []string{"vllm", "sidecar"},
		},
		"deleteEmptyDirData evicts the NPU pod and leaves non-NPU pods alone": {
			spec:        v1beta1.PodDeletionSpec{DeleteEmptyDirData: true, TimeoutSeconds: 5},
			pods:        []*corev1.Pod{newPod("vllm", true, true, rsOwner), newPod("sidecar", false, true, rsOwner)},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"vllm"},
			wantKept:    []string{"sidecar"},
		},
		"controller-less NPU pod needs force": {
			spec:       v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:       []*corev1.Pod{newPod("bare", true, false, nil)},
			wantState:  UpgradeStateSkipped,
			wantReason: []string{"declare no controller", ns + "/bare", "force=true"},
			wantKept:   []string{"bare"},
		},
		"force evicts the controller-less NPU pod": {
			spec:        v1beta1.PodDeletionSpec{Force: true, TimeoutSeconds: 5},
			pods:        []*corev1.Pod{newPod("bare", true, false, nil)},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"bare"},
		},
		"DaemonSet-managed NPU pod is reported by name": {
			spec:       v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:       []*corev1.Pod{newPod("ds-npu", true, false, dsOwner)},
			wantState:  UpgradeStateSkipped,
			wantReason: []string{"DaemonSet-managed", ns + "/ds-npu"},
			wantAbsent: []string{"<nil>"},
			wantKept:   []string{"ds-npu"},
		},
		// The reason this eviction stopped counting pods: a co-located infra
		// pod that mounts emptyDir is blocked by the helper and lands in its
		// error list, but it is not an NPU pod and must not hold the node.
		"non-NPU emptyDir pod does not block an evictable NPU pod": {
			spec:        v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:        []*corev1.Pod{newPod("npu-job", true, false, rsOwner), newPod("prometheus-0", false, true, rsOwner)},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"npu-job"},
			wantKept:    []string{"prometheus-0"},
		},
		"no NPU pods proceeds to pod-restart-required": {
			spec:      v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:      []*corev1.Pod{newPod("sidecar", false, true, rsOwner)},
			wantState: UpgradeStatePodRestartRequired,
			wantKept:  []string{"sidecar"},
		},
		// Left behind, this pod keeps /dev/rbln* open and the driver pod that
		// follows cannot unload the module, wedging the node.
		"NPU pod holding only a DRA claim is evicted": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{withClaim(newPod("dra-vllm", false, false, rsOwner), "npu-claim")},
			draObjs: []runtime.Object{
				deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
				claimForClass(ns, "npu-claim", npuDeviceClass),
			},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"dra-vllm"},
		},
		// The passthrough class carries no extended-resource bridge, so a VM
		// holding it is not a container-mode NPU pod to move.
		"passthrough DRA claim is left alone": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{withClaim(newPod("virt-launcher", false, false, rsOwner), "vfio-claim")},
			draObjs: []runtime.Object{
				deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
				deviceClass(vfioDeviceClass, nil),
				claimForClass(ns, "vfio-claim", vfioDeviceClass),
			},
			wantState: UpgradeStatePodRestartRequired,
			wantKept:  []string{"virt-launcher"},
		},
		"blocked DRA claim pod is named in the skip reason": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{withClaim(newPod("dra-vllm", false, true, rsOwner), "npu-claim")},
			draObjs: []runtime.Object{
				deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
				claimForClass(ns, "npu-claim", npuDeviceClass),
			},
			wantState:  UpgradeStateSkipped,
			wantReason: []string{"use emptyDir volumes", ns + "/dra-vllm"},
			wantKept:   []string{"dra-vllm"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			objs := []runtime.Object{npuDS}
			for _, pod := range tc.pods {
				objs = append(objs, pod)
			}
			objs = append(objs, tc.draObjs...)
			clientset := k8sfake.NewClientset(objs...)
			// No eviction subresource is advertised, so the helper deletes
			// pods directly instead of going through the eviction API.
			clientset.Resources = []*metav1.APIResourceList{{GroupVersion: "v1"}}

			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add corev1 scheme: %v", err)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   nodeName,
				Labels: map[string]string{UpgradeStateLabelKey: UpgradeStatePodDeletionRequired},
			}}
			ctrlClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
			pm := NewPodManager(clientset, NewNodeUpgradeStateProvider(ctrlClient, nil), npuFilter)

			spec := tc.spec
			err := pm.SchedulePodEviction(context.Background(), &PodManagerConfig{
				Nodes: []*corev1.Node{node}, DeletionSpec: &spec,
			})
			if err != nil {
				t.Fatalf("SchedulePodEviction: %v", err)
			}
			waitEvictionFinished(t, pm, nodeName)

			updated := &corev1.Node{}
			if err := ctrlClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("state = %q, want %q (reason %q)", got, tc.wantState, updated.Annotations[UpgradeSkipReasonAnnotationKey])
			}
			reason := updated.Annotations[UpgradeSkipReasonAnnotationKey]
			for _, want := range tc.wantReason {
				if !strings.Contains(reason, want) {
					t.Fatalf("skip reason %q missing %q", reason, want)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(reason, absent) {
					t.Fatalf("skip reason %q must not mention %q", reason, absent)
				}
			}
			for _, name := range tc.wantDeleted {
				_, err := clientset.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					t.Fatalf("pod %q should have been evicted, get err = %v", name, err)
				}
			}
			for _, name := range tc.wantKept {
				if _, err := clientset.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
					t.Fatalf("pod %q must be left alone, get err = %v", name, err)
				}
			}
		})
	}
}
