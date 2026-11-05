// Package npuusage observes Rebellions resource usage without deciding whether
// a pod should be evicted or a device operation should proceed. Pod phases,
// lookup failures and workload exclusions are handled by each caller.
//
// Keep this package and its tests identical in rbln-npu-operator,
// k8s-partition-manager and rbln-k8s-driver-manager. The binaries are released
// independently; the detector has no dependency on any component's policy.
package npuusage

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
)

const domain = "rebellions.ai"

func IsResourceName(name string) bool {
	return strings.HasPrefix(name, domain+"/")
}

func IsDriverName(name string) bool {
	return name == domain || strings.HasSuffix(name, "."+domain)
}

// ResourceNames includes container and passthrough names alike. Init containers
// include native sidecars; an exited init container is still reported because
// this function describes the pod's resource declarations, not process liveness.
func ResourceNames(pod *corev1.Pod) []string {
	var names []string
	for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, container := range containers {
			for _, resources := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
				for name, quantity := range resources {
					if quantity.Sign() > 0 && IsResourceName(string(name)) {
						names = append(names, string(name))
					}
				}
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// ClaimNames resolves direct, template-generated and scheduler-generated
// references. It returns all resolvable names even when another reference is
// unresolved, so the caller can apply its own lookup-failure policy.
func ClaimNames(pod *corev1.Pod) ([]string, error) {
	var names []string
	var result error
	add := func(name string) {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	for _, ref := range pod.Spec.ResourceClaims {
		name := ref.ResourceClaimName
		if name == nil {
			resolved := false
			for _, status := range pod.Status.ResourceClaimStatuses {
				if status.Name == ref.Name {
					name, resolved = status.ResourceClaimName, true
					break
				}
			}
			if !resolved {
				result = errors.Join(result, fmt.Errorf("pod %s/%s: resource claim %q is not resolved", pod.Namespace, pod.Name, ref.Name))
				continue
			}
			if name == nil {
				continue // an explicit nil status means no claim was needed
			}
		}
		if *name == "" {
			result = errors.Join(result, fmt.Errorf("pod %s/%s: resource claim %q has no name", pod.Namespace, pod.Name, ref.Name))
			continue
		}
		add(*name)
	}
	if ref := pod.Status.ExtendedResourceClaimStatus; ref != nil {
		if ref.ResourceClaimName == "" {
			result = errors.Join(result, fmt.Errorf("pod %s/%s: extended resource claim has no name", pod.Namespace, pod.Name))
		} else {
			add(ref.ResourceClaimName)
		}
	}
	return names, result
}

// ClaimUsage preserves both ownership evidence and class information. A caller
// may retain a configured class outside our domain, or exclude a passthrough
// class, without changing how the claim is inspected.
type ClaimUsage struct {
	Rebellions bool
	Allocated  bool
	Drivers    []string
	// Classes contains allocated classes, or all candidates before allocation.
	Classes []string
	// RequestedClasses also includes unselected FirstAvailable alternatives.
	RequestedClasses []string
}

// InspectClaim recognizes allocated devices by their driver's domain, including
// VFIO devices. Before allocation, class names in our domain are only evidence
// of a request; Allocated stays false. An allocated claim never falls back to
// an unselected class to infer its device owner.
func InspectClaim(claim *resourcev1.ResourceClaim) ClaimUsage {
	usage := ClaimUsage{Allocated: claim.Status.Allocation != nil}
	allocatedRequests := map[string]bool{}
	if usage.Allocated {
		for _, result := range claim.Status.Allocation.Devices.Results {
			allocatedRequests[result.Request] = true
			if !slices.Contains(usage.Drivers, result.Driver) {
				usage.Drivers = append(usage.Drivers, result.Driver)
			}
			usage.Rebellions = usage.Rebellions || IsDriverName(result.Driver)
		}
	}
	addClass := func(request, class string) {
		if class == "" {
			return
		}
		if !slices.Contains(usage.RequestedClasses, class) {
			usage.RequestedClasses = append(usage.RequestedClasses, class)
		}
		if !usage.Allocated || allocatedRequests[request] {
			if !slices.Contains(usage.Classes, class) {
				usage.Classes = append(usage.Classes, class)
			}
		}
		if !usage.Allocated {
			usage.Rebellions = usage.Rebellions || IsDriverName(class)
		}
	}
	for _, request := range claim.Spec.Devices.Requests {
		if request.Exactly != nil {
			addClass(request.Name, request.Exactly.DeviceClassName)
		}
		for _, alternative := range request.FirstAvailable {
			addClass(request.Name+"/"+alternative.Name, alternative.DeviceClassName)
		}
	}
	return usage
}
