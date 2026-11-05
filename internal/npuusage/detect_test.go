package npuusage

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func ptr(s string) *string { return &s }

func TestNameBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name             string
		resource, driver bool
	}{
		{"rebellions.ai/npu", true, false},
		{"rebellions.ai/RBLN-CA22_PF", true, false},
		{"rebellions.ai/ATOM_CA25_PT", true, false},
		{"rebellions.ai", false, true},
		{"npu.rebellions.ai", false, true},
		{"vfio-npu.rebellions.ai", false, true},
		{"npu.notrebellions.ai", false, false},
		{"rebellions.ai.example/npu", false, false},
		{"npu.rebellions.ai.example", false, false},
		{"example.com/rebellions.ai/npu", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if IsResourceName(tc.name) != tc.resource || IsDriverName(tc.name) != tc.driver {
				t.Fatalf("unexpected resource/driver match for %q", tc.name)
			}
		})
	}
}

func TestResourcesIncludeBothModesAndInitContainers(t *testing.T) {
	for _, name := range []string{"rebellions.ai/npu", "rebellions.ai/RBLN-CA22_PF", "rebellions.ai/ATOM_CA25_PT"} {
		for _, location := range []string{"container", "init", "sidecar"} {
			for _, limits := range []bool{false, true} {
				c := corev1.Container{}
				rl := corev1.ResourceList{corev1.ResourceName(name): resource.MustParse("1")}
				if limits {
					c.Resources.Limits = rl
				} else {
					c.Resources.Requests = rl
				}
				p := corev1.Pod{}
				if location == "container" {
					p.Spec.Containers = []corev1.Container{c}
				} else {
					if location == "sidecar" {
						always := corev1.ContainerRestartPolicyAlways
						c.RestartPolicy = &always
					}
					p.Spec.InitContainers = []corev1.Container{c}
				}
				if got := ResourceNames(&p); !slices.Equal(got, []string{name}) {
					t.Fatalf("%s/%s limits=%v: got %v", name, location, limits, got)
				}
			}
		}
	}
}

func TestResourcesRequirePositiveQuantityAndDeduplicate(t *testing.T) {
	p := corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			"rebellions.ai/npu":     resource.MustParse("0"),
			"rebellions.ai/unused":  resource.MustParse("0"),
			"rebellions.ai/invalid": resource.MustParse("-1"),
			"notrebellions.ai/npu":  resource.MustParse("1"),
		},
		Limits: corev1.ResourceList{"rebellions.ai/npu": resource.MustParse("1")},
	}}}, InitContainers: []corev1.Container{{Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"rebellions.ai/npu": resource.MustParse("1")},
	}}}}}
	if got := ResourceNames(&p); !slices.Equal(got, []string{"rebellions.ai/npu"}) {
		t.Fatalf("resources = %v", got)
	}
}

func TestClaimNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		refs     []corev1.PodResourceClaim
		statuses []corev1.PodResourceClaimStatus
		extended *corev1.PodExtendedResourceClaimStatus
		want     []string
		wantErr  bool
	}{
		{name: "direct wins over nil status", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimName: ptr("direct")}}, statuses: []corev1.PodResourceClaimStatus{{Name: "npu"}}, want: []string{"direct"}},
		{name: "generated", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimTemplateName: ptr("template")}}, statuses: []corev1.PodResourceClaimStatus{{Name: "npu", ResourceClaimName: ptr("generated")}}, want: []string{"generated"}},
		{name: "explicitly unused", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimTemplateName: ptr("template")}}, statuses: []corev1.PodResourceClaimStatus{{Name: "npu"}}},
		{name: "unresolved with another resolvable claim", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimTemplateName: ptr("template")}, {Name: "other", ResourceClaimName: ptr("known")}}, want: []string{"known"}, wantErr: true},
		{name: "empty direct name", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimName: ptr("")}}, wantErr: true},
		{name: "scheduler generated", extended: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "scheduled"}, want: []string{"scheduled"}},
		{name: "empty scheduler name", extended: &corev1.PodExtendedResourceClaimStatus{}, wantErr: true},
		{name: "duplicate references", refs: []corev1.PodResourceClaim{{Name: "npu", ResourceClaimName: ptr("shared")}}, extended: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "shared"}, want: []string{"shared"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := corev1.Pod{Spec: corev1.PodSpec{ResourceClaims: tc.refs}, Status: corev1.PodStatus{ResourceClaimStatuses: tc.statuses, ExtendedResourceClaimStatus: tc.extended}}
			got, err := ClaimNames(&p)
			if !slices.Equal(got, tc.want) || (err != nil) != tc.wantErr {
				t.Fatalf("names=%v error=%v, want %v/error=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestInspectClaimSeparatesOwnershipFromClassPolicy(t *testing.T) {
	const npuClass = "npu.rebellions.ai"
	const vfioClass = "vfio-npu.rebellions.ai"
	for _, tc := range []struct {
		name            string
		allocated       bool
		request, driver string
		wantOwner       bool
		wantClasses     []string
	}{
		{name: "pending candidates", wantOwner: true, wantClasses: []string{npuClass, vfioClass}},
		{name: "allocated container", allocated: true, request: "device/container", driver: "npu.rebellions.ai", wantOwner: true, wantClasses: []string{npuClass}},
		{name: "allocated passthrough", allocated: true, request: "device/vfio", driver: "npu.rebellions.ai", wantOwner: true, wantClasses: []string{vfioClass}},
		{name: "root driver", allocated: true, request: "device/vfio", driver: "rebellions.ai", wantOwner: true, wantClasses: []string{vfioClass}},
		{name: "class cannot override foreign allocation", allocated: true, request: "device/container", driver: "other.example", wantClasses: []string{npuClass}},
		{name: "similar domain", allocated: true, request: "device/container", driver: "npu.notrebellions.ai", wantClasses: []string{npuClass}},
		{name: "allocation without spec request", allocated: true, request: "external", driver: "npu.rebellions.ai", wantOwner: true},
		{name: "empty allocation", allocated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := resourcev1.ResourceClaim{Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
				Name: "device", FirstAvailable: []resourcev1.DeviceSubRequest{{Name: "container", DeviceClassName: npuClass}, {Name: "vfio", DeviceClassName: vfioClass}},
			}}}}}
			if tc.allocated {
				claim.Status.Allocation = &resourcev1.AllocationResult{}
				if tc.request != "" {
					claim.Status.Allocation.Devices.Results = []resourcev1.DeviceRequestAllocationResult{{Request: tc.request, Driver: tc.driver}}
				}
			}
			got := InspectClaim(&claim)
			if got.Rebellions != tc.wantOwner || got.Allocated != tc.allocated || !slices.Equal(got.Classes, tc.wantClasses) || !slices.Equal(got.RequestedClasses, []string{npuClass, vfioClass}) {
				t.Fatalf("unexpected usage: %+v", got)
			}
		})
	}
}

func TestAllocatedDriverIdentifiesCustomClass(t *testing.T) {
	claim := resourcev1.ResourceClaim{Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
		Name: "device", Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: "custom.example.com"},
	}}}}}
	claim.Status.Allocation = &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{Request: "device", Driver: "npu.rebellions.ai"}}}}
	got := InspectClaim(&claim)
	if !got.Rebellions || !slices.Equal(got.Classes, []string{"custom.example.com"}) {
		t.Fatalf("unexpected usage: %+v", got)
	}
}
