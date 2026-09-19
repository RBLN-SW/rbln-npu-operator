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

// npuDeviceClassNames returns the DRA DeviceClasses whose devices are NPUs this
// upgrade has to free: the ones bridged to a rebellions.ai/* extended resource.
//
// The bridge is the discriminator, not the class name. The operator renders the
// container-mode class with ExtendedResourceName set and the passthrough class
// without one (internal/clusterpolicy/components/dra_kubelet_plugin.go), so
// reading it here tracks a customized draKubeletPlugin.driverName and leaves a
// KubeVirt VM's passthrough claim alone. It also keeps the criterion identical
// to npuPodSpecFilter's — "holds a rebellions.ai/* resource" — with the claim
// the second spelling of the same sentence.
func npuDeviceClassNames(ctx context.Context, k8sInterface kubernetes.Interface) ([]string, error) {
	list, err := k8sInterface.ResourceV1().DeviceClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list DRA device classes: %w", err)
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		extendedResource := list.Items[i].Spec.ExtendedResourceName
		if extendedResource != nil && strings.HasPrefix(*extendedResource, consts.RBLNResourceNamePrefix) {
			names = append(names, list.Items[i].Name)
		}
	}
	return names, nil
}

// podsHoldingNPUDeviceClaim returns the pods that hold an NPU through a DRA
// ResourceClaim rather than a resource request. Such a pod carries no
// rebellions.ai/* entry in its container resources at all, so npuPodSpecFilter
// cannot see it — and a pod it misses keeps /dev/rbln* open, so the driver pod
// that follows cannot unload the module and wedges the node.
//
// A claim that cannot be read counts as not holding an NPU: one missing RBAC
// rule must not park every node in the cluster, and the unread claim's pod
// surfaces as a driver pod that cannot unload the module.
func podsHoldingNPUDeviceClaim(
	ctx context.Context,
	k8sInterface kubernetes.Interface,
	pods []corev1.Pod,
	deviceClasses []string,
) map[string]struct{} {
	if len(deviceClasses) == 0 {
		return nil
	}

	holders := map[string]struct{}{}
	// One Get per claim, not per pod: pods of one workload commonly share a
	// claim, and a node runs many of them.
	verdicts := map[string]bool{}
	for i := range pods {
		pod := &pods[i]
		if len(pod.Spec.ResourceClaims) == 0 || !podRunningOrPending(pod) {
			continue
		}
		for _, claimName := range podResourceClaimNames(pod) {
			key := pod.Namespace + "/" + claimName
			holdsNPU, decided := verdicts[key]
			if !decided {
				holdsNPU = claimUsesNPUDeviceClass(ctx, k8sInterface, pod.Namespace, claimName, deviceClasses)
				verdicts[key] = holdsNPU
			}
			if holdsNPU {
				holders[podKey(pod)] = struct{}{}
				break
			}
		}
	}
	return holders
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
