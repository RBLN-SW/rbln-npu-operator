// Package drivermanager is the operator's half of its contract with the
// rbln-k8s-driver-manager binary: what the operator renders into the init
// container that runs it. Two DaemonSets run that container, the driver pod
// (reconcile-driver-state) and the vfio-manager pod (reconcile-vfio-state),
// and both empty a node of NPU pods under one policy, so the policy and its
// env rendering live here rather than in either component.
package drivermanager

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// NPUPodEvictionPolicy is what k8s-driver-manager is allowed to do when it has
// to empty a node of NPU pods itself: on the driver path while driver
// auto-upgrade is off (with it on, the binary defers to the upgrade controller
// and evicts nothing), and on the vfio path before it binds the node's NPUs to
// vfio-pci. The values mirror upgradePolicy.podDeletion so every eviction
// obeys one policy.
type NPUPodEvictionPolicy struct {
	Force              bool
	DeleteEmptyDirData bool
	// DeviceClass names the container-mode DRA DeviceClass whose claims mark a
	// pod as an NPU consumer, tracking draKubeletPlugin.driverName.
	DeviceClass string
}

// ResolveNPUPodEvictionPolicy reads the policy off the cluster policy spec.
// upgradePolicy.podDeletion is the source even though that block otherwise
// governs the operator-driven rollout: every eviction frees the same node for
// the same reason, and a node must not become unupgradable, or unconvertible
// to vm-passthrough, simply because auto-upgrade is off. autoUpgrade is
// deliberately not consulted. A nil spec keeps the strictest behaviour.
func ResolveNPUPodEvictionPolicy(spec *rblnv1beta1.RBLNClusterPolicySpec) NPUPodEvictionPolicy {
	policy := NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass}
	if spec == nil {
		return policy
	}
	if driverName := spec.DRAKubeletPlugin.DriverName; driverName != "" {
		policy.DeviceClass = driverName
	}
	upgradePolicy := spec.Driver.UpgradePolicy
	if upgradePolicy == nil || upgradePolicy.PodDeletion == nil {
		return policy
	}
	policy.Force = upgradePolicy.PodDeletion.Force
	policy.DeleteEmptyDirData = upgradePolicy.PodDeletion.DeleteEmptyDirData
	return policy
}

// NPUPodEvictionEnv renders the policy as the env vars k8s-driver-manager
// binds, with its eviction switched on. Both init containers render exactly
// this list, so the two cannot drift apart.
func NPUPodEvictionEnv(policy NPUPodEvictionPolicy) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "ENABLE_NPU_POD_EVICTION", Value: "true"},
		{Name: "NPU_POD_EVICTION_FORCE", Value: strconv.FormatBool(policy.Force)},
		{Name: "NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA", Value: strconv.FormatBool(policy.DeleteEmptyDirData)},
		{Name: "NPU_POD_EVICTION_DEVICE_CLASS", Value: policy.DeviceClass},
	}
}
