package upgrade

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// evictionBlock is the drain-helper filter that kept an NPU pod out of the
// deletable set.
type evictionBlock int

const (
	blockedByEmptyDir evictionBlock = iota
	blockedByNoController
	blockedByDaemonSet
	blockedUnknown
)

// maxNamedPodsPerBlock caps the pods named per clause. markNodeUpgradeSkipped
// truncates the reason at 400 characters, and an uncapped list pushes the
// remedy, the only actionable part of the message, past that limit.
const maxNamedPodsPerBlock = 3

// blockedNPUPods returns the NPU pods the eviction must remove that the drain
// helper did not mark deletable.
func blockedNPUPods(npuPods, deletable []corev1.Pod) []corev1.Pod {
	ok := make(map[string]struct{}, len(deletable))
	for i := range deletable {
		ok[podKey(&deletable[i])] = struct{}{}
	}
	blocked := make([]corev1.Pod, 0, len(npuPods))
	for i := range npuPods {
		if _, deletable := ok[podKey(&npuPods[i])]; !deletable {
			blocked = append(blocked, npuPods[i])
		}
	}
	return blocked
}

func podKey(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

// podKeys names every blocked pod for the log. The skip reason caps its list at
// maxNamedPodsPerBlock, so the log is the only place the full set survives.
func podKeys(pods []corev1.Pod) []string {
	keys := make([]string, 0, len(pods))
	for i := range pods {
		keys = append(keys, podKey(&pods[i]))
	}
	return keys
}

// classifyEvictionBlock re-derives the helper's verdict in the helper's own
// filter order (DaemonSet, local storage, unreplicated): the helper reports
// per-pod status only for deletable pods, and its error list also covers
// non-NPU pods it never would have evicted. The order must track the helper's,
// which short-circuits on the first filter that blocks a pod -- classifying by
// a later filter would name a remedy that cannot unblock it.
//
// The helper also skips mirror pods, right after the DaemonSet filter. That
// step is left out on purpose: reaching it needs a static pod that requests an
// NPU on an NPU worker node, and the Node ownerReference the kubelet stamps on
// a mirror pod keeps it out of the controller-less branch regardless.
func classifyEvictionBlock(pod *corev1.Pod, spec *v1beta1.PodDeletionSpec) evictionBlock {
	controller := metav1.GetControllerOf(pod)
	switch {
	case controller != nil && controller.Kind == "DaemonSet":
		return blockedByDaemonSet
	case !spec.DeleteEmptyDirData && hasEmptyDir(pod):
		return blockedByEmptyDir
	case controller == nil && !spec.Force:
		return blockedByNoController
	default:
		return blockedUnknown
	}
}

func hasEmptyDir(pod *corev1.Pod) bool {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].EmptyDir != nil {
			return true
		}
	}
	return false
}

// evictionBlockReason renders the skip reason: one clause per block kind,
// naming the pods and the remedy. Helper errors are attached only to pods no
// filter explains.
func evictionBlockReason(blocked []corev1.Pod, spec *v1beta1.PodDeletionSpec, helperErrs []error) string {
	byKind := map[evictionBlock][]string{}
	for i := range blocked {
		kind := classifyEvictionBlock(&blocked[i], spec)
		byKind[kind] = append(byKind[kind], podKey(&blocked[i]))
	}

	clauses := make([]string, 0, len(byKind))
	for _, kind := range []evictionBlock{blockedByEmptyDir, blockedByNoController, blockedByDaemonSet, blockedUnknown} {
		names := byKind[kind]
		if len(names) == 0 {
			continue
		}
		sort.Strings(names)
		clauses = append(clauses, describeEvictionBlock(kind, names, helperErrs))
	}
	return strings.Join(clauses, "; ")
}

// namePods renders at most maxNamedPodsPerBlock names and summarizes the rest,
// so the clause length stays bounded by the cap rather than by the node's pod
// count. The caller still reports the true total.
func namePods(names []string) string {
	if len(names) <= maxNamedPodsPerBlock {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s, and %d more",
		strings.Join(names[:maxNamedPodsPerBlock], ", "), len(names)-maxNamedPodsPerBlock)
}

func describeEvictionBlock(kind evictionBlock, names []string, helperErrs []error) string {
	n := len(names)
	list := namePods(names)
	switch kind {
	case blockedByEmptyDir:
		return fmt.Sprintf("%d NPU pod(s) use emptyDir volumes: %s "+
			"(set upgradePolicy.podDeletion.deleteEmptyDirData=true to evict them, or move the workloads and retry)",
			n, list)
	case blockedByNoController:
		return fmt.Sprintf("%d NPU pod(s) declare no controller: %s "+
			"(set upgradePolicy.podDeletion.force=true to evict them, or delete the pods and retry)",
			n, list)
	case blockedByDaemonSet:
		return fmt.Sprintf("%d NPU pod(s) are DaemonSet-managed and cannot be evicted: %s "+
			"(exclude the node from that DaemonSet and retry)",
			n, list)
	default:
		reason := fmt.Sprintf("%d NPU pod(s) were not evictable: %s", n, list)
		if err := errors.Join(helperErrs...); err != nil {
			reason += fmt.Sprintf(" (drain helper: %v)", err)
		}
		return reason
	}
}
