package upgrade

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/draclass"
	"github.com/RBLN-SW/rbln-npu-operator/internal/npuusage"
)

// npuDeviceClassNames returns the DRA DeviceClasses whose claims mark a pod as
// an NPU pod this upgrade has to free. The container-mode class is known by
// name: npuDeviceClass is the fixed npu.rebellions.ai class the operator
// hands k8s-driver-manager as NPU_POD_EVICTION_DEVICE_CLASS, so both eviction
// paths judge a claim by one criterion.
//
// The name is the discriminator because it is the only one every API server
// persists. The operator renders the container-mode class with an
// extended-resource bridge and the passthrough class without one
// (internal/clusterpolicy/components/dra_kubelet_plugin.go), but an API server
// without the DRAExtendedResource feature gate — off by default through
// Kubernetes 1.35 — drops Spec.ExtendedResourceName on write. A matcher keyed
// on the bridge then saw no NPU class at all and let every claim holder ride
// through the rollout still holding /dev/rbln*. The bridge is kept as a hint
// only: any other class bridged to a rebellions.ai/* resource is added, so a
// hand-made class on a cluster that does persist the field is still honored.
// Both native and vfio classes are included: driver replacement now releases
// passthrough devices before restoring their partition layout.
func npuDeviceClassNames(ctx context.Context, k8sInterface kubernetes.Interface, npuDeviceClass string) ([]string, error) {
	list, err := k8sInterface.ResourceV1().DeviceClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list DRA device classes: %w", err)
	}

	names := draclass.Names(npuDeviceClass)
	for i := range list.Items {
		class := &list.Items[i]
		if slices.Contains(names, class.Name) {
			continue
		}
		extendedResource := class.Spec.ExtendedResourceName
		if extendedResource != nil && npuusage.IsResourceName(*extendedResource) {
			names = append(names, class.Name)
		}
	}
	return names, nil
}

// resolveNPUDeviceClasses resolves the NPU DeviceClasses for one eviction pass.
// The DeviceClass list only adds hints, so a failure to read it is not fatal: a
// cluster that does not serve the DRA API answers 404 and has no claims to
// find, and any other failure must not cost the rollout the configured class,
// or a transient read error would let every claim holder ride through the
// eviction. A pod the matcher still misses surfaces as a driver pod that
// cannot unload the module.
func resolveNPUDeviceClasses(ctx context.Context, k8sInterface kubernetes.Interface, npuDeviceClass string) []string {
	names, err := npuDeviceClassNames(ctx, k8sInterface, npuDeviceClass)
	switch {
	case err == nil:
		return names
	case apierrors.IsNotFound(err):
		log.FromContext(ctx).V(consts.VDebug).Info(
			"DRA device classes are not served; NPU pods holding a ResourceClaim will not be evicted")
		return nil
	default:
		log.FromContext(ctx).Error(err,
			"Failed to list DRA device classes; using the configured native and VFIO classes",
			"deviceClass", npuDeviceClass)
		return draclass.Names(npuDeviceClass)
	}
}

// npuClaimMatcher reports whether a pod holds an NPU through a DRA
// ResourceClaim rather than a resource request. Such a pod carries no
// rebellions.ai/* entry in its container resources at all, so npuPodSpecFilter
// cannot see it — and a pod it misses keeps /dev/rbln* open, so the driver pod
// that follows cannot unload the module and wedges the node.
//
// It answers pod by pod rather than from a snapshot of the node's pods: the
// drain helper lists the node again after the first pass, and a pod that lands
// in between must be judged by the same criterion. Verdicts are cached per
// claim, not per pod — pods of one workload commonly share a claim, and a node
// runs many of them — which also keeps it to one Get per claim. One matcher
// serves one node's eviction on that node's goroutine; it is not safe for
// concurrent use.
//
// An unreadable or unresolved claim prevents the caller from advancing to
// driver replacement. The next eviction pass retries its API observation.
type npuClaimMatcher struct {
	k8sInterface  kubernetes.Interface
	deviceClasses []string
	verdicts      map[string]bool
	err           error
}

func newNPUClaimMatcher(k8sInterface kubernetes.Interface, deviceClasses []string) *npuClaimMatcher {
	return &npuClaimMatcher{
		k8sInterface:  k8sInterface,
		deviceClasses: deviceClasses,
		verdicts:      map[string]bool{},
	}
}

func (m *npuClaimMatcher) holdsNPUClaim(ctx context.Context, pod *corev1.Pod) bool {
	if len(m.deviceClasses) == 0 || !podRunningOrPending(pod) {
		return false
	}
	names, err := npuusage.ClaimNames(pod)
	if err != nil {
		m.err = err
	}
	for _, claimName := range names {
		key := pod.Namespace + "/" + claimName
		holdsNPU, decided := m.verdicts[key]
		if !decided {
			claim, err := m.k8sInterface.ResourceV1().ResourceClaims(pod.Namespace).Get(ctx, claimName, metav1.GetOptions{})
			if err != nil {
				m.err = fmt.Errorf("resolve NPU claim %s/%s: %w", pod.Namespace, claimName, err)
			} else {
				holdsNPU = claimRequestsDeviceClass(claim, m.deviceClasses)
			}
			m.verdicts[key] = holdsNPU
		}
		if holdsNPU {
			return true
		}
	}
	return false
}

// claimRequestsDeviceClass applies the upgrade policy to the shared detector.
// Allocated claims match the configured driver or the selected classes only;
// pending claims match every candidate, including FirstAvailable alternatives.
// Native and VFIO consumers must both release devices before driver replacement.
func claimRequestsDeviceClass(claim *resourcev1.ResourceClaim, deviceClasses []string) bool {
	usage := npuusage.InspectClaim(claim)
	for _, driver := range usage.Drivers {
		for _, class := range deviceClasses {
			if driver == strings.TrimPrefix(class, "vfio-") {
				return true
			}
		}
	}
	for _, class := range usage.Classes {
		if slices.Contains(deviceClasses, class) {
			return true
		}
	}
	return false
}

// Finished pods no longer hold the device; only Running and Pending ones do,
// matching npuPodSpecFilter's own phase gate.
func podRunningOrPending(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending
}
