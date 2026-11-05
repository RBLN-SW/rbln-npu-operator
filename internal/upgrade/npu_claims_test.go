package upgrade

import (
	"context"
	"errors"
	"slices"
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

var operatorDeviceClasses = []string{
	npuDeviceClass, vfioDeviceClass,
	"npu-vf1.rebellions.ai", "npu-vf4.rebellions.ai",
	"vfio-npu-vf1.rebellions.ai", "vfio-npu-vf4.rebellions.ai",
}

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

// The container-mode class is matched by name — npu.rebellions.ai,
// the value k8s-driver-manager is handed as NPU_POD_EVICTION_DEVICE_CLASS —
// because the name is the one discriminator every API server persists. An API
// server without the DRAExtendedResource feature gate drops the
// extended-resource bridge the operator renders on the class, and a matcher
// keyed on the bridge saw no NPU class at all. The bridge stays a hint for
// classes the operator did not render; native and VFIO PF/VF names are always known.
func TestNPUDeviceClassNames(t *testing.T) {
	tests := map[string]struct {
		npuDeviceClass string
		classes        []runtime.Object
		want           []string
	}{
		"configured class matches by name when the API server pruned the bridge": {
			npuDeviceClass: npuDeviceClass,
			classes: []runtime.Object{
				deviceClass(npuDeviceClass, nil),
				deviceClass(vfioDeviceClass, nil),
			},
			want: operatorDeviceClasses,
		},
		"configured class is listed once when its bridge is persisted": {
			npuDeviceClass: npuDeviceClass,
			classes: []runtime.Object{
				deviceClass(npuDeviceClass, strPtr("rebellions.ai/npu")),
				deviceClass(vfioDeviceClass, nil),
			},
			want: operatorDeviceClasses,
		},
		"explicit class matches independently of the driver identity": {
			npuDeviceClass: "npu.example.com",
			classes: []runtime.Object{
				deviceClass("npu.example.com", nil),
				deviceClass("vfio-npu.example.com", nil),
				deviceClass(npuDeviceClass, nil),
			},
			want: []string{"npu.example.com", "vfio-npu.example.com", "npu-vf1.example.com", "npu-vf4.example.com", "vfio-npu-vf1.example.com", "vfio-npu-vf4.example.com"},
		},
		"another class bridged to a rebellions.ai resource is a hint": {
			npuDeviceClass: npuDeviceClass,
			classes: []runtime.Object{
				deviceClass(npuDeviceClass, nil),
				deviceClass("npu-shared.example.com", strPtr("rebellions.ai/npu")),
				deviceClass("gpu.example.com", strPtr("example.com/gpu")),
			},
			want: append(slices.Clone(operatorDeviceClasses), "npu-shared.example.com"),
		},
		"passthrough class is included once even when bridged": {
			npuDeviceClass: npuDeviceClass,
			classes:        []runtime.Object{deviceClass(vfioDeviceClass, strPtr("rebellions.ai/npu"))},
			want:           operatorDeviceClasses,
		},
		"configured class is matched before its DeviceClass exists": {
			npuDeviceClass: npuDeviceClass,
			want:           operatorDeviceClasses,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clientset := k8sfake.NewClientset(tc.classes...)

			got, err := npuDeviceClassNames(context.Background(), clientset, tc.npuDeviceClass)
			if err != nil {
				t.Fatalf("npuDeviceClassNames: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("device classes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNPUDeviceClassNamesPropagatesListFailure(t *testing.T) {
	clientset := k8sfake.NewClientset()
	clientset.PrependReactor("list", "deviceclasses",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(
				schema.GroupResource{Group: "resource.k8s.io", Resource: "deviceclasses"}, "")
		})

	_, err := npuDeviceClassNames(context.Background(), clientset, npuDeviceClass)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("err = %v, want a NotFound the caller can recognize as a cluster without DRA", err)
	}
}

// The DeviceClass list only adds hints. A cluster without the DRA API has no
// claims to match, so nothing is returned; any other list failure must not
// cost the rollout the configured class, or a transient read error would let
// every claim holder ride through the eviction.
func TestResolveNPUDeviceClasses(t *testing.T) {
	tests := map[string]struct {
		listErr error
		want    []string
	}{
		"DRA API not served": {
			listErr: apierrors.NewNotFound(
				schema.GroupResource{Group: "resource.k8s.io", Resource: "deviceclasses"}, ""),
			want: nil,
		},
		"list failure keeps the configured class": {
			listErr: apierrors.NewInternalError(errors.New("etcd timeout")),
			want:    operatorDeviceClasses,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clientset := k8sfake.NewClientset()
			clientset.PrependReactor("list", "deviceclasses",
				func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.listErr
				})

			got := resolveNPUDeviceClasses(context.Background(), clientset, npuDeviceClass)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("resolveNPUDeviceClasses() = %v, want %v", got, tc.want)
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
		"pending passthrough class requires a configured match": {
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

// An unreadable claim must prevent driver replacement until a later pass
// can determine whether the workload needs to release an NPU.
func TestNPUClaimMatcherBlocksOnUnreadableClaim(t *testing.T) {
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
	if matcher.err == nil {
		t.Fatal("unreadable claim must block driver replacement")
	}
}

func TestNPUClaimMatcherDetectsAllocatedSchedulerClaims(t *testing.T) {
	for _, class := range operatorDeviceClasses {
		t.Run(class, func(t *testing.T) {
			claim := claimForClass("ns", "scheduled", class)
			claim.Status.Allocation = &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{Request: "npu", Driver: npuDeviceClass}}}}
			pod := claimingPod("ns", "workload", "scheduled")
			pod.Spec.ResourceClaims = nil
			pod.Status.ExtendedResourceClaimStatus = &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "scheduled"}
			matcher := newNPUClaimMatcher(k8sfake.NewClientset(claim), []string{npuDeviceClass})
			if !matcher.holdsNPUClaim(context.Background(), pod) {
				t.Fatalf("allocated %s scheduler claim must be evicted", class)
			}
		})
	}
}

func TestNPUClaimMatcherReferenceErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		refs     []corev1.PodResourceClaim
		statuses []corev1.PodResourceClaimStatus
		extended *corev1.PodExtendedResourceClaimStatus
		wantErr  bool
	}{
		{
			name: "two references share one resolved claim",
			refs: []corev1.PodResourceClaim{
				{Name: "first", ResourceClaimName: strPtr("known")},
				{Name: "second", ResourceClaimName: strPtr("known")},
			},
		},
		{
			name:     "explicit unused template is resolved",
			refs:     []corev1.PodResourceClaim{{Name: "unused", ResourceClaimTemplateName: strPtr("template")}},
			statuses: []corev1.PodResourceClaimStatus{{Name: "unused"}},
		},
		{
			name:     "scheduler claim cannot mask unresolved template",
			refs:     []corev1.PodResourceClaim{{Name: "pending", ResourceClaimTemplateName: strPtr("template")}},
			extended: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "known"},
			wantErr:  true,
		},
		{
			name:     "empty scheduler claim blocks replacement",
			extended: &corev1.PodExtendedResourceClaimStatus{},
			wantErr:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := claimingPod("ns", "workload", "known")
			pod.Spec.ResourceClaims = tc.refs
			pod.Status.ResourceClaimStatuses = tc.statuses
			pod.Status.ExtendedResourceClaimStatus = tc.extended
			matcher := newNPUClaimMatcher(k8sfake.NewClientset(claimForClass("ns", "known", "other.example.com")), operatorDeviceClasses)
			if matcher.holdsNPUClaim(context.Background(), pod) {
				t.Fatal("foreign claim must not be evicted")
			}
			if (matcher.err != nil) != tc.wantErr {
				t.Fatalf("error=%v, want error=%v", matcher.err, tc.wantErr)
			}
		})
	}
}

func TestClaimEligibilityUsesAllocatedAlternative(t *testing.T) {
	for _, tc := range []struct {
		name, request, driver string
		allocated             bool
		want                  bool
	}{
		{name: "pending NPU alternative", want: true},
		{name: "selected GPU ignores NPU alternative", allocated: true, request: "device/gpu", driver: "gpu.example.com"},
		{name: "allocated alias uses configured driver", allocated: true, request: "device/alias", driver: npuDeviceClass, want: true},
		{name: "empty allocation has no device", allocated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := &resourcev1.ResourceClaim{Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
					Name: "device", FirstAvailable: []resourcev1.DeviceSubRequest{
						{Name: "gpu", DeviceClassName: "gpu.example.com"},
						{Name: "npu", DeviceClassName: npuDeviceClass},
						{Name: "alias", DeviceClassName: "alias.example.com"},
					},
				}}},
			}}
			if tc.allocated {
				claim.Status.Allocation = &resourcev1.AllocationResult{}
				if tc.request != "" {
					claim.Status.Allocation.Devices.Results = []resourcev1.DeviceRequestAllocationResult{{Request: tc.request, Driver: tc.driver}}
				}
			}
			if got := claimRequestsDeviceClass(claim, operatorDeviceClasses); got != tc.want {
				t.Fatalf("eligible=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestPendingPFAndVFClaimsDoNotRequireExtendedResourceBridge(t *testing.T) {
	classes := resolveNPUDeviceClasses(context.Background(), k8sfake.NewClientset(), npuDeviceClass)
	for _, class := range operatorDeviceClasses {
		t.Run(class, func(t *testing.T) {
			if !claimRequestsDeviceClass(claimForClass("ns", "pending", class), classes) {
				t.Fatalf("pending operator class %s must be eligible without an allocation or bridge", class)
			}
		})
	}
}
