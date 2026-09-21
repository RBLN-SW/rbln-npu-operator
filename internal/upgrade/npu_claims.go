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

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// npuDeviceClassNames returns the DRA DeviceClasses whose claims mark a pod as
// an NPU pod this upgrade has to free. The container-mode class is known by
// name: npuDeviceClass is draKubeletPlugin.driverName, the value the operator
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
// The passthrough class vfio-<driverName> is never one, bridged or not: the
// device a KubeVirt VM holds through it sits on vfio-pci, not on the driver
// being replaced.
func npuDeviceClassNames(ctx context.Context, k8sInterface kubernetes.Interface, npuDeviceClass string) ([]string, error) {
	list, err := k8sInterface.ResourceV1().DeviceClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list DRA device classes: %w", err)
	}

	names := make([]string, 0, len(list.Items)+1)
	names = append(names, npuDeviceClass)
	passthroughClass := passthroughDeviceClassName(npuDeviceClass)
	for i := range list.Items {
		class := &list.Items[i]
		if class.Name == npuDeviceClass || class.Name == passthroughClass {
			continue
		}
		extendedResource := class.Spec.ExtendedResourceName
		if extendedResource != nil && strings.HasPrefix(*extendedResource, consts.RBLNResourceNamePrefix) {
			names = append(names, class.Name)
		}
	}
	return names, nil
}

// passthroughDeviceClassName mirrors the DRA kubelet plugin patcher's
// vfioClassName: the passthrough class is the container-mode class name with
// a "vfio-" prefix.
func passthroughDeviceClassName(npuDeviceClass string) string {
	return "vfio-" + npuDeviceClass
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
			"Failed to list DRA device classes; only ResourceClaims on the configured NPU class will be evicted",
			"deviceClass", npuDeviceClass)
		return []string{npuDeviceClass}
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
// A claim that cannot be read counts as not holding an NPU: one missing RBAC
// rule must not park every node in the cluster, and the unread claim's pod
// surfaces as a driver pod that cannot unload the module.
type npuClaimMatcher struct {
	k8sInterface  kubernetes.Interface
	deviceClasses []string
	verdicts      map[string]bool
}

func newNPUClaimMatcher(k8sInterface kubernetes.Interface, deviceClasses []string) *npuClaimMatcher {
	return &npuClaimMatcher{
		k8sInterface:  k8sInterface,
		deviceClasses: deviceClasses,
		verdicts:      map[string]bool{},
	}
}

func (m *npuClaimMatcher) holdsNPUClaim(ctx context.Context, pod *corev1.Pod) bool {
	if len(m.deviceClasses) == 0 || len(pod.Spec.ResourceClaims) == 0 || !podRunningOrPending(pod) {
		return false
	}
	for _, claimName := range podResourceClaimNames(pod) {
		key := pod.Namespace + "/" + claimName
		holdsNPU, decided := m.verdicts[key]
		if !decided {
			holdsNPU = claimUsesNPUDeviceClass(ctx, m.k8sInterface, pod.Namespace, claimName, m.deviceClasses)
			m.verdicts[key] = holdsNPU
		}
		if holdsNPU {
			return true
		}
	}
	return false
}

// podResourceClaimNames resolves the ResourceClaim objects a pod refers to. A
// direct spec reference is the name outright; the pod status is consulted only
// for a template-generated claim, whose real name lives there and nowhere else.
// That is the precedence k8s.io/dynamic-resource-allocation's resourceclaim.Name
// applies. Letting the status win instead would drop a named claim the moment
// anything wrote a nameless status entry for it, and a dropped claim here is an
// NPU pod that survives the eviction still holding /dev/rbln*. A status entry
// with no name means that entry needed no claim.
func podResourceClaimNames(pod *corev1.Pod) []string {
	names := make([]string, 0, len(pod.Spec.ResourceClaims))
	for _, claim := range pod.Spec.ResourceClaims {
		name := claim.ResourceClaimName
		if name == nil {
			for _, status := range pod.Status.ResourceClaimStatuses {
				if status.Name == claim.Name {
					name = status.ResourceClaimName
					break
				}
			}
		}
		if name != nil && *name != "" {
			names = append(names, *name)
		}
	}
	return names
}

func claimUsesNPUDeviceClass(
	ctx context.Context,
	k8sInterface kubernetes.Interface,
	namespace, name string,
	deviceClasses []string,
) bool {
	claim, err := k8sInterface.ResourceV1().ResourceClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		// Raced with claim creation or deletion; nothing to hold.
		return false
	case err != nil:
		log.FromContext(ctx).Error(err,
			"Cannot read a DRA ResourceClaim; a pod holding an NPU through it will not be evicted",
			"namespace", namespace, "name", name)
		return false
	}
	return claimRequestsDeviceClass(claim, deviceClasses)
}

func claimRequestsDeviceClass(claim *resourcev1.ResourceClaim, deviceClasses []string) bool {
	for _, request := range claim.Spec.Devices.Requests {
		if request.Exactly != nil && slices.Contains(deviceClasses, request.Exactly.DeviceClassName) {
			return true
		}
		for _, alternative := range request.FirstAvailable {
			if slices.Contains(deviceClasses, alternative.DeviceClassName) {
				return true
			}
		}
	}
	return false
}

// Finished pods no longer hold the device; only Running and Pending ones do,
// matching npuPodSpecFilter's own phase gate.
func podRunningOrPending(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending
}
