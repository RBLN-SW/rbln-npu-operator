/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/drivermanager"
	k8sutil "github.com/RBLN-SW/rbln-npu-operator/internal/utils/k8s"
)

type vfioManagerPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNVFIOManagerSpec
	podDefaults *rblnv1beta1.PodDefaultsSpec
	// evictionPolicy is what the k8s-driver-manager init container may do to
	// the node's container-mode NPU pods before vfio-manager binds vfio-pci.
	evictionPolicy drivermanager.NPUPodEvictionPolicy
}

func NewVFIOManagerPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.VFIOManager)
	return &vfioManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNVFIOManagerName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigVMPassthrough,
		},
		desiredSpec:    &synced,
		podDefaults:    cpSpec.PodDefaults,
		evictionPolicy: drivermanager.ResolveNPUPodEvictionPolicy(cpSpec),
	}
}

func (h *vfioManagerPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileVFIOManagerRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *vfioManagerPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.VDebug).Info("Cleaning up disabled component", "component", "VFIO Manager")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *vfioManagerPatcher) handleClusterRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.name}}
	_, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				// Cluster-scoped, not namespaced: the k8s-driver-manager init
				// container lists every pod on the node and evicts the
				// container-mode NPU pods in their own namespaces before
				// vfio-pci is bound.
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/eviction"},
				Verbs:     []string{"create"},
			},
			{
				// A pod holding its NPU through a DRA ResourceClaim has no
				// rebellions.ai/* request to match on; the binary reads the
				// claim to find it.
				APIGroups: []string{"resource.k8s.io"},
				Resources: []string{"resourceclaims"},
				Verbs:     []string{"get"},
			},
		}
		return ctrl.SetControllerReference(owner, role, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager ClusterRole", "name", h.name)
		return err
	}
	return nil
}

func (h *vfioManagerPatcher) handleClusterRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name}}
	_, err := controllerutil.CreateOrPatch(ctx, h.client, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     h.name,
		}
		crb.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      h.name,
			Namespace: h.namespace,
		}}
		return ctrl.SetControllerReference(owner, crb, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager ClusterRoleBinding", "name", h.name)
		return err
	}
	return nil
}

func (h *vfioManagerPatcher) reconcileVFIOManagerRBAC(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace}}
	if _, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"get", "list", "watch"},
			},
		}
		return ctrl.SetControllerReference(owner, role, h.scheme)
	}); err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager Role", "name", h.name)
		return err
	}

	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace}}
	if _, err := controllerutil.CreateOrPatch(ctx, h.client, rb, func() error {
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     h.name,
		}
		rb.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      h.name,
			Namespace: h.namespace,
		}}
		return ctrl.SetControllerReference(owner, rb, h.scheme)
	}); err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager RoleBinding", "name", h.name)
		return err
	}
	return nil
}

func (h *vfioManagerPatcher) handleConfigMap(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace},
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = map[string]string{
			"vfio-manage.sh": `#!/bin/bash
# Copyright 2026 Rebellions Inc.
#
# vfio-manage.sh hands Rebellions NPUs to the vfio-pci driver so that they can
# be passed through to virtual machines, and reports whether that has happened.
#
#   vfio-manage.sh bind       (-a|--all | -d|--device-id <PCI_ADDR>)
#   vfio-manage.sh check_bind (-a|--all | -d|--device-id <PCI_ADDR>)
#
# bind is idempotent: an NPU that is already on vfio-pci is left alone and one
# held by another driver is released first. check_bind exits non-zero when a
# selected NPU is not on vfio-pci.
set -Eeuo pipefail

readonly SYSFS_PCI=/sys/bus/pci/devices
readonly VFIO_BIND=/sys/bus/pci/drivers/vfio-pci/bind
readonly RBLN_VENDOR_ID=0x1eff   # Rebellions Inc.
readonly NPU_CLASS_ID=0x120000   # processing accelerator

usage() {
	cat >&2 <<USAGE
Usage: $0 COMMAND [OPTIONS]

Commands:
  bind        [-a|--all] [-d|--device-id <PCI_ADDR>]
  check_bind  [-a|--all] [-d|--device-id <PCI_ADDR>]
  help        [-h|--help]
USAGE
	exit 0
}

# ---- sysfs queries ----------------------------------------------------------

# driver_of ADDR prints the name of the driver bound to ADDR, or nothing.
driver_of() {
	local link=$SYSFS_PCI/$1/driver
	[[ -e $link ]] || return 0
	basename "$(readlink -f "$link")"
}

# has_npu_class ADDR succeeds when ADDR reports the NPU PCI class.
has_npu_class() {
	local class
	class=$(<"$(readlink -f "$SYSFS_PCI/$1/class")")
	[[ $class == "$NPU_CLASS_ID" ]]
}

is_bound_to_vfio() {
	[[ $(driver_of "$1") == vfio-pci ]]
}

# rebellions_npus prints the PCI address of every Rebellions NPU, one per line.
rebellions_npus() {
	local dev addr
	for dev in "$SYSFS_PCI"/*; do
		[[ -r $dev/vendor ]] || continue
		addr=${dev##*/}
		if [[ $(<"$dev/vendor") == "$RBLN_VENDOR_ID" ]] && has_npu_class "$addr"; then
			echo "$addr"
		fi
	done
}

# ---- bind -------------------------------------------------------------------

# unbind_from_other_driver ADDR releases ADDR from any driver but vfio-pci.
unbind_from_other_driver() {
	local addr=$1 current
	current=$(driver_of "$addr")
	[[ -n $current && $current != vfio-pci ]] || return 0
	echo "unbinding device $addr from driver $current"
	echo "$addr" > "$SYSFS_PCI/$addr/driver/unbind"
	echo "" > "$SYSFS_PCI/$addr/driver_override"
}

# bind_pci_device ADDR makes vfio-pci the driver of ADDR.
bind_pci_device() {
	local addr=$1
	if is_bound_to_vfio "$addr"; then
		echo "device $addr already bound to vfio-pci"
		return 0
	fi
	unbind_from_other_driver "$addr"
	echo "binding device $addr"
	echo vfio-pci > "$SYSFS_PCI/$addr/driver_override"
	echo "$addr" > "$VFIO_BIND"
}

bind_one() {
	if ! has_npu_class "$1"; then
		echo "device $1 is not a npu!"
		return 0
	fi
	bind_pci_device "$1"
}

bind_all() {
	local dev vendor
	for dev in "$SYSFS_PCI"/*; do
		read -r vendor < "$dev/vendor"
		if [[ $vendor == "$RBLN_VENDOR_ID" ]]; then
			bind_one "${dev##*/}"
		fi
	done
}

# ---- check ------------------------------------------------------------------

check_one() {
	if ! has_npu_class "$1"; then
		echo "device $1 is not a Rebellions NPU!"
		return 0
	fi
	if is_bound_to_vfio "$1"; then
		echo "Device $1 is bound to vfio-pci"
		return 0
	fi
	echo "Device $1 is not bound to vfio-pci"
	return 1
}

check_all() {
	local addr total=0 missing=0
	while read -r addr; do
		total=$((total + 1))
		if is_bound_to_vfio "$addr"; then
			echo "Device $addr is bound to vfio-pci"
		else
			echo "Device $addr is not bound to vfio-pci"
			missing=$((missing + 1))
		fi
	done < <(rebellions_npus)
	if (( total == 0 )); then
		echo "No Rebellions NPU devices found."
		return 0
	fi
	if (( missing > 0 )); then
		return 1
	fi
	echo "All $total Rebellions NPU devices are bound to VFIO-PCI."
}

# ---- entry point ------------------------------------------------------------

handle_bind() {
	chroot /host modprobe vfio-pci
	if [[ -n $DEVICE_ID ]]; then
		bind_one "$DEVICE_ID"
	elif [[ $ALL_DEVICES == true ]]; then
		bind_all
	else
		usage
	fi
}

handle_check_bind() {
	if [[ -n $DEVICE_ID ]]; then
		check_one "$DEVICE_ID"
	elif [[ $ALL_DEVICES == true ]]; then
		check_all
	else
		usage
	fi
}

main() {
	(( $# > 0 )) || usage
	local command=$1
	shift
	DEVICE_ID=""
	ALL_DEVICES=false
	while (( $# > 0 )); do
		case $1 in
			-a|--all)
				ALL_DEVICES=true
				shift
				;;
			-d|--device-id)
				if (( $# < 2 )); then
					echo "Error: missing value for $1" >&2
					exit 2
				fi
				DEVICE_ID=$2
				shift 2
				;;
			-h|--help)
				usage
				;;
			--)
				shift
				break
				;;
			-*)
				echo "Unknown option: $1" >&2
				usage
				;;
			*)
				echo "Unexpected argument: $1" >&2
				usage
				;;
		esac
	done
	case $command in
		bind)       handle_bind ;;
		check_bind) handle_check_bind ;;
		help)       usage ;;
		*)
			echo "Unknown command: $command" >&2
			usage
			;;
	esac
}

main "$@"`,
		}
		return ctrl.SetControllerReference(cp, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager ConfigMap")
		return err
	}
	k8sutil.LogReconcileResult(h.log, "Reconciled VFIOManager ConfigMap", res, "namespace", cm.Namespace, "name", cm.Name)
	return nil
}

// driverManagerInitEnv renders the k8s-driver-manager init container's env.
// The eviction block is the one the driver pod renders too: on a node whose
// NPUs are still bound to the rebellions driver, the binary cordons the node
// and evicts its container-mode NPU pods (pods requesting exactly
// rebellions.ai/npu, or holding a DRA claim against the container-mode
// DeviceClass) before vfio-manager binds vfio-pci. What keeps a running VM
// safe is the binary, not this env: a KubeVirt VM holds a passthrough resource
// name and the vfio DeviceClass, neither of which is matched, and a node
// already on vfio-pci is not touched at all. A pod requesting a
// product-specific resource name is not matched either; it still fails the
// vfio-mode readiness check, which names the holding process. User env comes
// last so it can override any of these.
func (h *vfioManagerPatcher) driverManagerInitEnv(userEnv []corev1.EnvVar) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "spec.nodeName"}}},
		{Name: "OPERATOR_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
	}
	env = append(env, drivermanager.NPUPodEvictionEnv(h.evictionPolicy)...)
	env = append(env, corev1.EnvVar{Name: "PROC_ROOT", Value: "/host/proc"})
	return append(env, userEnv...)
}

func (h *vfioManagerPatcher) buildDriverUninstallInitContainer() *corev1.Container {
	dm := h.desiredSpec.DriverManager
	return k8sutil.NewContainerBuilder().
		WithName(consts.DriverManagerInitContainerName).
		WithImage(k8sutil.ComposeImageReference(dm.Registry, dm.Image), dm.Version, dm.ImagePullPolicy).
		WithCommands([]string{"driver-manager"}).
		WithArgs([]string{"reconcile-vfio-state"}).
		WithEnvs(h.driverManagerInitEnv(dm.Env)).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: "host-sys", MountPath: "/sys"},
			{
				Name:             "host-root",
				MountPath:        "/host",
				ReadOnly:         true,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
}

func (h *vfioManagerPatcher) buildPodSpec() *corev1.PodSpec {
	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.vfio-manager": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(podDefaultsPriorityClassName(h.podDefaults)).
		WithTerminationGracePeriodSeconds(30).
		WithVolumes([]corev1.Volume{
			{
				Name: h.name,
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: h.name + "-config"},
					DefaultMode:          ptr(int32(448)),
				}},
			},
			hostPathVolume("host-sys", "/sys", corev1.HostPathDirectory),
			hostPathVolume("host-root", "/", corev1.HostPathDirectory),
		}).
		WithInitContainers([]*corev1.Container{h.buildDriverUninstallInitContainer()}).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithCommands([]string{"/bin/bash", "-c"}).
				WithArgs([]string{`/bin/vfio-manage.sh bind --all || exit $?
# Keep bash as PID 1 and use wait so termination signals interrupt the idle wait.
trap 'exit 0' TERM INT
sleep inf &
wait $!`}).
				WithResources(h.desiredSpec.Resources, "100m", "200Mi").
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: h.name, MountPath: "/bin/vfio-manage.sh", SubPath: "vfio-manage.sh", ReadOnly: true},
					{Name: "host-sys", MountPath: "/sys"},
					{Name: "host-root", MountPath: "/host"},
				}).
				WithSecurityContext(&corev1.SecurityContext{
					RunAsUser:      ptr(int64(0)),
					Privileged:     ptr(true),
					SELinuxOptions: &corev1.SELinuxOptions{Level: "s0"},
				}).
				Build(),
		}).
		Build()
}

func (h *vfioManagerPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec()

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	sel := selectorLabels(consts.RBLNVFIOManagerName)

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(sel).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		applyUpdateStrategy(ds, h.desiredSpec.UpdateStrategy)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager DaemonSet")
		return err
	}
	k8sutil.LogReconcileResult(h.log, "Reconciled VFIOManager DaemonSet", res, "namespace", ds.Namespace, "name", ds.Name)
	return nil
}
