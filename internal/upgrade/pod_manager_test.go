package upgrade

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestGetPodDriverConfigDigest(t *testing.T) {
	pm := &PodManager{}

	t.Run("returns the digest stamped on the driver-manager init container", func(t *testing.T) {
		pod := &corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{
			Name: "k8s-driver-manager",
			Env:  []corev1.EnvVar{{Name: consts.DriverConfigDigestEnv, Value: "abc123"}},
		}}}}
		if got := pm.GetPodDriverConfigDigest(pod); got != "abc123" {
			t.Fatalf("digest = %q, want %q", got, "abc123")
		}
	})

	// A pod rendered before the digest existed must read as out of sync, not
	// as an error that strands the node.
	t.Run("returns empty when the pod carries no digest", func(t *testing.T) {
		pod := &corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "k8s-driver-manager"}}}}
		if got := pm.GetPodDriverConfigDigest(pod); got != "" {
			t.Fatalf("digest = %q, want empty", got)
		}
	})
}

func TestGetDaemonSetDriverConfigDigest(t *testing.T) {
	pm := &PodManager{}

	t.Run("returns the digest from the pod template", func(t *testing.T) {
		ds := &appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{
				Name: "k8s-driver-manager",
				Env:  []corev1.EnvVar{{Name: consts.DriverConfigDigestEnv, Value: "abc123"}},
			}},
		}}}}
		got, err := pm.GetDaemonSetDriverConfigDigest(ds)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "abc123" {
			t.Fatalf("digest = %q, want %q", got, "abc123")
		}
	})

	// The operator stamps every driver DaemonSet it renders; a missing digest
	// is an operator bug, not a legacy object.
	t.Run("errors when the template carries no digest", func(t *testing.T) {
		ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "rbln-driver-pool"}}
		if _, err := pm.GetDaemonSetDriverConfigDigest(ds); err == nil {
			t.Fatal("expected error for missing digest")
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
	tests := map[string]struct {
		state      string
		wantState  string
		wantReason bool
	}{
		"node still in pod-deletion-required is parked": {
			state: UpgradeStatePodDeletionRequired, wantState: UpgradeStateSkipped, wantReason: true,
		},
		// The teardown ran while the eviction was in flight; the verdict is
		// of a rollout that no longer exists and must not resurrect the label.
		"node torn down meanwhile is left alone": {
			state: "", wantState: "",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add corev1 scheme: %v", err)
			}
			labels := map[string]string{}
			if tc.state != "" {
				labels[UpgradeStateLabelKey] = tc.state
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "evict-worker", Labels: labels}}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
			pm := &PodManager{nodeUpgradeStateProvider: NewNodeUpgradeStateProvider(k8sClient, nil)}

			// The goroutine holds the node as it was when the eviction started.
			started := node.DeepCopy()
			started.Labels = map[string]string{UpgradeStateLabelKey: UpgradeStatePodDeletionRequired}
			pm.markNodeUpgradeSkippedAsync(context.Background(), *started, "pod eviction failed: PDB webapp-pdb")

			updated := &corev1.Node{}
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: node.Name}, updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("state = %q, want %q", got, tc.wantState)
			}
			_, reason := updated.Annotations[UpgradeSkipReasonAnnotationKey]
			if reason != tc.wantReason {
				t.Fatalf("skip reason present = %v, want %v", reason, tc.wantReason)
			}
		})
	}
}

func waitEvictionFinished(t *testing.T, pm *PodManager, nodeName string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, running := pm.nodesInProgress.Load(nodeName); !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("eviction goroutine did not finish in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPodWorkersReserveNodesUntilCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%t", fail), func(t *testing.T) {
			pm := &PodManager{}
			nodes := []*corev1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "one"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "two"}},
			}
			started := make(chan string, 64)
			release := make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(func() {
				finish()
				for _, node := range nodes {
					waitEvictionFinished(t, pm, node.Name)
				}
			})
			task := func(_ context.Context, node *corev1.Node) error {
				started <- node.Name
				<-release
				if fail {
					return fmt.Errorf("worker failed")
				}
				return nil
			}
			var callers sync.WaitGroup
			for range 32 {
				callers.Add(1)
				go func() {
					defer callers.Done()
					pm.schedule(context.Background(), nodes, task)
				}()
			}
			callsDone := make(chan struct{})
			go func() { callers.Wait(); close(callsDone) }()
			select {
			case <-callsDone:
			case <-time.After(5 * time.Second):
				t.Fatal("scheduling waited for an in-flight worker")
			}
			seen := make(map[string]bool)
			for range nodes {
				select {
				case name := <-started:
					if seen[name] {
						t.Fatalf("duplicate worker for %s", name)
					}
					seen[name] = true
				case <-time.After(5 * time.Second):
					t.Fatal("workers for distinct nodes did not run concurrently")
				}
			}
			select {
			case name := <-started:
				t.Fatalf("duplicate worker for %s", name)
			case <-time.After(100 * time.Millisecond):
			}
			finish()
			for _, node := range nodes {
				waitEvictionFinished(t, pm, node.Name)
			}
			// Success and failure both release the reservation for a later pass.
			pm.schedule(context.Background(), nodes[:1], task)
			select {
			case name := <-started:
				if name != nodes[0].Name {
					t.Fatalf("unexpected worker %s", name)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("completed work left the node permanently reserved")
			}
			waitEvictionFinished(t, pm, nodes[0].Name)
		})
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
		spec    v1beta1.PodDeletionSpec
		pods    []*corev1.Pod
		draObjs []runtime.Object
		// setup adjusts the clientset before the eviction runs.
		setup func(t *testing.T, cs *k8sfake.Clientset)
		// tornDown removes the node's state label before the eviction runs,
		// as an autoUpgrade-off teardown racing the goroutine would.
		tornDown    bool
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
		// An API server without the DRAExtendedResource feature gate (off by
		// default through Kubernetes 1.35) drops the extended-resource bridge
		// the operator renders on the class. The claim holder must still be
		// recognized by the class name.
		"NPU pod holding a DRA claim is evicted when the API server pruned the class bridge": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{withClaim(newPod("dra-vllm", false, false, rsOwner), "npu-claim")},
			draObjs: []runtime.Object{
				deviceClass(npuDeviceClass, nil),
				claimForClass(ns, "npu-claim", npuDeviceClass),
			},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"dra-vllm"},
		},
		// A VM holding the passthrough class is not a container-mode NPU pod
		// to move; its device sits on vfio-pci, not on the driver being replaced.
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
		// The drain helper lists the node's pods again after the first pass;
		// a claim-holding pod that lands in between must be judged by the
		// same criterion, not skipped because a snapshot never saw it.
		"DRA claim pod that appears after the first listing is evicted": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{newPod("vllm", true, false, rsOwner)},
			draObjs: []runtime.Object{
				deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
				claimForClass(ns, "npu-claim", npuDeviceClass),
			},
			setup: func(t *testing.T, cs *k8sfake.Clientset) {
				lists := 0
				cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
					lists++
					if lists == 2 {
						late := withClaim(newPod("dra-late", false, false, rsOwner), "npu-claim")
						if err := cs.Tracker().Add(late); err != nil {
							t.Errorf("add late pod: %v", err)
						}
					}
					return false, nil, nil
				})
			},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"vllm", "dra-late"},
		},
		// The goroutine outlives the reconcile that started it. A teardown
		// that ran meanwhile has already returned the node to service; the
		// verdict must not put the label back on it.
		"teardown during the eviction leaves the node unlabeled": {
			spec:        v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods:        []*corev1.Pod{newPod("vllm", true, false, rsOwner)},
			tornDown:    true,
			wantState:   "",
			wantDeleted: []string{"vllm"},
		},
		// A cluster without the DRA API has no claims to find; the pod-spec
		// filter must still run.
		"pod-spec NPU pod is evicted when the DRA API is not served": {
			spec: v1beta1.PodDeletionSpec{TimeoutSeconds: 5},
			pods: []*corev1.Pod{newPod("vllm", true, false, rsOwner)},
			setup: func(_ *testing.T, cs *k8sfake.Clientset) {
				cs.PrependReactor("list", "deviceclasses", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewNotFound(
						schema.GroupResource{Group: "resource.k8s.io", Resource: "deviceclasses"}, "")
				})
			},
			wantState:   UpgradeStatePodRestartRequired,
			wantDeleted: []string{"vllm"},
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
			if tc.setup != nil {
				tc.setup(t, clientset)
			}

			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add corev1 scheme: %v", err)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   nodeName,
				Labels: map[string]string{UpgradeStateLabelKey: UpgradeStatePodDeletionRequired},
			}}
			stored := node.DeepCopy()
			if tc.tornDown {
				stored.Labels = nil
			}
			ctrlClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stored).Build()
			pm := NewPodManager(clientset, NewNodeUpgradeStateProvider(ctrlClient, nil), npuFilter)

			spec := tc.spec
			err := pm.SchedulePodEviction(context.Background(), &PodManagerConfig{
				Nodes: []*corev1.Node{node}, DeletionSpec: &spec, NPUDeviceClass: npuDeviceClass,
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

func TestTransientPodListErrorMustRetry(t *testing.T) {
	m := newTestManager(t)
	n := newNodeUpgradeState("worker", UpgradeStatePodDeletionRequired, "rev1")
	registerNodes(t, m, n.Node)
	cs := k8sfake.NewClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("transient API outage")
	})
	pm := NewPodManager(cs, m.nodeUpgradeStateProvider, func(corev1.Pod) bool { return true })
	if err := pm.SchedulePodEviction(context.Background(), &PodManagerConfig{Nodes: []*corev1.Node{n.Node}, DeletionSpec: &v1beta1.PodDeletionSpec{TimeoutSeconds: 1}}); err != nil {
		t.Fatal(err)
	}
	waitEvictionFinished(t, pm, n.Node.Name)
	stored := &corev1.Node{}
	if err := m.k8sClient.Get(context.Background(), types.NamespacedName{Name: n.Node.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Labels[UpgradeStateLabelKey] != UpgradeStatePodDeletionRequired {
		t.Fatalf("transient list failure parked node: %s, reason=%s", stored.Labels[UpgradeStateLabelKey], stored.Annotations[UpgradeSkipReasonAnnotationKey])
	}
}

func TestOmittedDeletionPolicyDoesNotImplicitlyProceed(t *testing.T) {
	m := newTestManager(t)
	n := newNodeUpgradeState("worker", UpgradeStatePodDeletionRequired, "rev1")
	registerNodes(t, m, n.Node)
	pm := NewPodManager(k8sfake.NewClientset(), m.nodeUpgradeStateProvider, func(corev1.Pod) bool { return true })
	err := pm.SchedulePodEviction(context.Background(), &PodManagerConfig{Nodes: []*corev1.Node{n.Node}})
	waitEvictionFinished(t, pm, n.Node.Name)
	if err == nil {
		stored, readErr := m.nodeUpgradeStateProvider.GetNode(context.Background(), n.Node.Name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		t.Fatalf("omitted podDeletion was accepted with zero-value policy: resulting state=%q", stored.Labels[UpgradeStateLabelKey])
	}
}

func TestDigestLocation(t *testing.T) {
	pm := &PodManager{}
	pod := &corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{
		{Name: "first", Env: []corev1.EnvVar{{Name: consts.DriverConfigDigestEnv, Value: "first-digest"}}},
		{Name: "k8s-driver-manager", Env: []corev1.EnvVar{{Name: consts.DriverConfigDigestEnv, Value: "second-digest"}}},
	}}}
	if got := pm.GetPodDriverConfigDigest(pod); got != "first-digest" {
		t.Fatalf("digest precedence changed: %q", got)
	}
	ds := &appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: pod.Spec}}}
	if got, err := pm.GetDaemonSetDriverConfigDigest(ds); err != nil || got != "first-digest" {
		t.Fatalf("DaemonSet digest=%q, err=%v", got, err)
	}
}

func TestPodArrivingAfterDrainSelectionDoesNotCausePolicySkip(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	n := newNodeUpgradeState("worker", UpgradeStatePodDeletionRequired, "rev1")
	registerNodes(t, m, n.Node)
	first := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "workloads"},
		Spec:       corev1.PodSpec{NodeName: n.Node.Name},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	cs := k8sfake.NewClientset(first)
	cs.Resources = []*metav1.APIResourceList{{GroupVersion: "v1"}}
	lists := 0
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		if lists != 2 {
			return false, nil, nil
		}
		// Return the helper's snapshot, then make another eligible pod visible
		// to subsequent reads. Its absence from that snapshot is not a block.
		snapshot, err := cs.Tracker().List(corev1.SchemeGroupVersion.WithResource("pods"), corev1.SchemeGroupVersion.WithKind("Pod"), "workloads")
		if err != nil {
			return true, nil, err
		}
		late := first.DeepCopy()
		late.Name = "late"
		if err := cs.Tracker().Add(late); err != nil {
			return true, nil, err
		}
		return true, snapshot, nil
	})
	pm := NewPodManager(cs, m.nodeUpgradeStateProvider, func(p corev1.Pod) bool { return p.Status.Phase == corev1.PodRunning })
	if err := pm.SchedulePodEviction(ctx, &PodManagerConfig{
		Nodes: []*corev1.Node{n.Node}, DeletionSpec: &v1beta1.PodDeletionSpec{Force: true, TimeoutSeconds: 1},
	}); err != nil {
		t.Fatal(err)
	}
	waitEvictionFinished(t, pm, n.Node.Name)
	stored, err := m.nodeUpgradeStateProvider.GetNode(ctx, n.Node.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Labels[UpgradeStateLabelKey]; got != UpgradeStatePodRestartRequired {
		t.Fatalf("state=%q, reason=%q", got, stored.Annotations[UpgradeSkipReasonAnnotationKey])
	}
	if _, err := cs.CoreV1().Pods(first.Namespace).Get(ctx, first.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("selected pod was not evicted: %v", err)
	}
	if _, err := cs.CoreV1().Pods(first.Namespace).Get(ctx, "late", metav1.GetOptions{}); err != nil {
		t.Fatalf("pod outside the helper's selection was touched: %v", err)
	}
}

func TestEvictionBatchResolvesDeviceClassesOnce(t *testing.T) {
	m := newTestManager(t)
	one := newNodeUpgradeState("worker-one", UpgradeStatePodDeletionRequired, "rev1")
	two := newNodeUpgradeState("worker-two", UpgradeStatePodDeletionRequired, "rev1")
	registerNodes(t, m, one.Node, two.Node)
	cs := k8sfake.NewClientset()
	pm := NewPodManager(cs, m.nodeUpgradeStateProvider, func(corev1.Pod) bool { return false })
	if err := pm.SchedulePodEviction(context.Background(), &PodManagerConfig{
		Nodes: []*corev1.Node{one.Node, two.Node}, DeletionSpec: &v1beta1.PodDeletionSpec{}, NPUDeviceClass: "npu.example.com",
	}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*corev1.Node{one.Node, two.Node} {
		waitEvictionFinished(t, pm, n.Name)
		stored, err := m.nodeUpgradeStateProvider.GetNode(context.Background(), n.Name)
		if err != nil || stored.Labels[UpgradeStateLabelKey] != UpgradeStatePodRestartRequired {
			t.Fatalf("node %s did not complete eviction: node=%+v, err=%v", n.Name, stored, err)
		}
	}
	lists := 0
	for _, action := range cs.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "deviceclasses" {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("device class lists=%d, want one per batch", lists)
	}
}

func TestPodCompletionRejectsMissingPolicyBeforeReadingPods(t *testing.T) {
	cs := k8sfake.NewClientset()
	pm := NewPodManager(cs, nil, nil)
	for _, config := range []*PodManagerConfig{nil, {Nodes: []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}}}} {
		if err := pm.ScheduleCheckOnPodCompletion(context.Background(), config); err == nil {
			t.Fatal("missing completion policy must be rejected")
		}
	}
	if len(cs.Actions()) != 0 {
		t.Fatal("missing policy must not turn into an all-pods query")
	}
}
