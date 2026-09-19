package upgrade

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	npuDeviceClass  = "npu.rebellions.ai"
	vfioDeviceClass = "vfio-npu.rebellions.ai"
)

func strPtr(s string) *string { return &s }

func deviceClass(name string, extendedResource *string) *resourcev1.DeviceClass {
	return &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       resourcev1.DeviceClassSpec{ExtendedResourceName: extendedResource},
	}
}

func claimForClass(namespace, name, className string) *resourcev1.ResourceClaim {
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{
			Requests: []resourcev1.DeviceRequest{{
				Name:    "npu",
				Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: className},
			}},
		}},
	}
}

func claimingPod(namespace, name, claimName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{
			ResourceClaims: []corev1.PodResourceClaim{{
				Name:              "npu",
				ResourceClaimName: strPtr(claimName),
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// The extended-resource bridge, not the class name, is what marks a DeviceClass
// as one this upgrade must free: it tracks a customized driverName and excludes
// the passthrough class the operator deliberately leaves unbridged.
func TestNPUDeviceClassNames(t *testing.T) {
	clientset := k8sfake.NewClientset(
		deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
		deviceClass(vfioDeviceClass, nil),
		deviceClass("gpu.example.com", strPtr("example.com/gpu")),
	)

	got, err := npuDeviceClassNames(context.Background(), clientset)
	if err != nil {
		t.Fatalf("npuDeviceClassNames: %v", err)
	}
	if len(got) != 1 || got[0] != npuDeviceClass {
		t.Fatalf("device classes = %v, want [%s]", got, npuDeviceClass)
	}
}

func TestNPUDeviceClassNamesPropagatesListFailure(t *testing.T) {
	clientset := k8sfake.NewClientset()
	clientset.PrependReactor("list", "deviceclasses",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(
				schema.GroupResource{Group: "resource.k8s.io", Resource: "deviceclasses"}, "")
		})

	_, err := npuDeviceClassNames(context.Background(), clientset)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("err = %v, want a NotFound the caller can recognize as a cluster without DRA", err)
	}
}

func TestPodResourceClaimNames(t *testing.T) {
	tests := map[string]struct {
		pod  corev1.Pod
		want []string
	}{
		"claim named in the spec": {
			pod:  *claimingPod("ns", "pod", "shared-claim"),
			want: []string{"shared-claim"},
		},
		// A nameless status entry must not erase a direct reference: only a
		// template-generated claim takes its name from the status.
		"direct reference wins over a nameless status entry": {
			pod: corev1.Pod{
				Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{
					Name: "npu", ResourceClaimName: strPtr("shared-claim"),
				}}},
				Status: corev1.PodStatus{ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "npu"}}},
			},
			want: []string{"shared-claim"},
		},
		// A template-generated claim has no name in the spec; the kubelet
		// records the generated one in the pod status.
		"template-generated claim is named only in the status": {
			pod: corev1.Pod{
				Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{
					Name:                      "npu",
					ResourceClaimTemplateName: strPtr("npu-template"),
				}}},
				Status: corev1.PodStatus{ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{
					Name: "npu", ResourceClaimName: strPtr("pod-npu-abcde"),
				}}},
			},
			want: []string{"pod-npu-abcde"},
		},
		// A status entry with no name means the entry needed no claim.
		"status entry without a name yields nothing": {
			pod: corev1.Pod{
				Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{
					Name: "npu", ResourceClaimTemplateName: strPtr("npu-template"),
				}}},
				Status: corev1.PodStatus{ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "npu"}}},
			},
			want: []string{},
		},
		"no claims": {pod: corev1.Pod{}, want: []string{}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := podResourceClaimNames(&tc.pod)
			if len(got) != len(tc.want) {
				t.Fatalf("names = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("names = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestClaimRequestsDeviceClass(t *testing.T) {
	tests := map[string]struct {
		claim resourcev1.ResourceClaim
		want  bool
	}{
		"exact request on the NPU class": {
			claim: *claimForClass("ns", "c", npuDeviceClass),
			want:  true,
		},
		"passthrough class is not an NPU the container-mode upgrade must free": {
			claim: *claimForClass("ns", "c", vfioDeviceClass),
			want:  false,
		},
		"any alternative of a first-available request counts": {
			claim: resourcev1.ResourceClaim{Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
					Name: "npu",
					FirstAvailable: []resourcev1.DeviceSubRequest{
						{Name: "big", DeviceClassName: "gpu.example.com"},
						{Name: "small", DeviceClassName: npuDeviceClass},
					},
				}}},
			}},
			want: true,
		},
		"no requests": {claim: resourcev1.ResourceClaim{}, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := claimRequestsDeviceClass(&tc.claim, []string{npuDeviceClass}); got != tc.want {
				t.Fatalf("claimRequestsDeviceClass() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNPUClaimMatcher(t *testing.T) {
	const ns = "workloads"

	finished := claimingPod(ns, "completed", "npu-claim")
	finished.Status.Phase = corev1.PodSucceeded

	clientset := k8sfake.NewClientset(
		claimForClass(ns, "npu-claim", npuDeviceClass),
		claimForClass(ns, "vfio-claim", vfioDeviceClass),
	)
	claimReads := 0
	clientset.PrependReactor("get", "resourceclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			claimReads++
			return false, nil, nil
		})

	// A passthrough VM, a pod whose claim is gone, a finished pod and a pod
	// with no claim at all must not be pulled into the eviction.
	tests := map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"claim on the NPU class":        {pod: claimingPod(ns, "vllm", "npu-claim"), want: true},
		"second pod sharing that claim": {pod: claimingPod(ns, "vllm-replica", "npu-claim"), want: true},
		"passthrough claim":             {pod: claimingPod(ns, "virt-launcher", "vfio-claim"), want: false},
		"claim that no longer exists":   {pod: claimingPod(ns, "dangling", "deleted-claim"), want: false},
		"finished pod":                  {pod: finished, want: false},
		"no claim at all": {
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "plain"},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: false,
		},
	}

	matcher := newNPUClaimMatcher(clientset, []string{npuDeviceClass})
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := matcher.holdsNPUClaim(context.Background(), tc.pod); got != tc.want {
				t.Fatalf("holdsNPUClaim(%s) = %v, want %v", tc.pod.Name, got, tc.want)
			}
		})
	}
	// One Get per distinct claim, not per pod: three claims are referenced
	// by five pods, and the finished pod is gated before any read.
	if claimReads != 3 {
		t.Fatalf("resource claim reads = %d, want 3", claimReads)
	}
}

// Without device classes there is nothing to match, and the lookup must not
// issue a single API call.
func TestNPUClaimMatcherSkipsLookupWithoutDeviceClasses(t *testing.T) {
	clientset := k8sfake.NewClientset()
	clientset.PrependReactor("get", "resourceclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			t.Error("ResourceClaims must not be read when no NPU device class exists")
			return true, nil, errors.New("unexpected call")
		})

	matcher := newNPUClaimMatcher(clientset, nil)
	if matcher.holdsNPUClaim(context.Background(), claimingPod("ns", "vllm", "npu-claim")) {
		t.Fatal("holdsNPUClaim() = true, want false without device classes")
	}
}

// One missing RBAC rule or a flaky read must not park every node in the
// cluster: an unreadable claim counts as not holding an NPU.
func TestNPUClaimMatcherTreatsUnreadableClaimAsAbsent(t *testing.T) {
	clientset := k8sfake.NewClientset()
	clientset.PrependReactor("get", "resourceclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "resource.k8s.io", Resource: "resourceclaims"}, "npu-claim",
				errors.New("no permission"))
		})

	matcher := newNPUClaimMatcher(clientset, []string{npuDeviceClass})
	if matcher.holdsNPUClaim(context.Background(), claimingPod("ns", "vllm", "npu-claim")) {
		t.Fatal("holdsNPUClaim() = true, want false for a claim that cannot be read")
	}
}
