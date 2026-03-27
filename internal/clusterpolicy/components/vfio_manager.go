package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

type vfioManagerPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNVFIOManagerSpec
}

func NewVFIOManagerPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.VFIOManager)
	return &vfioManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             cpSpec.BaseName + "-" + consts.RBLNVFIOManagerName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec: &synced,
	}
}

func (h *vfioManagerPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
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
	h.log.Info("WARNING: VFIO Manager is disabled. Remove all VFIO Manager resources")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *vfioManagerPatcher) handleConfigMap(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace},
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = map[string]string{
			"vfio-manage.sh": `#!/bin/bash

set -Eeuo pipefail

usage()
{
	cat >&2 <<EOF
Usage: $0 COMMAND [OPTIONS]

Commands:
  bind        [-a|--all] [-d|--device-id <PCI_ADDR>]
  unbind      [-a|--all] [-d|--device-id <PCI_ADDR>]
  check_bind  [-a|--all] [-d|--device-id <PCI_ADDR>]
  help        [-h|--help]
EOF
	exit 0
}

unbind_from_driver() {
	local npu=$1
	local existing_driver_name
	local existing_driver

	[ -e "/sys/bus/pci/devices/$npu/driver" ] || return 0

	existing_driver=$(readlink -f "/sys/bus/pci/devices/$npu/driver")
	existing_driver_name=$(basename "$existing_driver")

	echo "unbinding device $npu from driver $existing_driver_name"
	echo "$npu" > "$existing_driver/unbind"
	echo > /sys/bus/pci/devices/$npu/driver_override
}

# unbind device from non vfio-pci driver
unbind_from_other_driver() {
	local npu=$1
	local existing_driver_name
	local existing_driver

	[ -e "/sys/bus/pci/devices/$npu/driver" ] || return 0

	existing_driver=$(readlink -f "/sys/bus/pci/devices/$npu/driver")
	existing_driver_name=$(basename "$existing_driver")

	# return if bound to vfio-pci
	[ "$existing_driver_name" != "vfio-pci" ] || return 0
	echo "unbinding device $npu from driver $existing_driver_name"
	echo "$npu" > "$existing_driver/unbind"
	echo > /sys/bus/pci/devices/$npu/driver_override
}

is_target_npu_device() {
	local npu=$1
	# make sure device class is for target npu
	device_class_file=$(readlink -f "/sys/bus/pci/devices/$npu/class")
	device_class=$(cat "$device_class_file")

	# Check if device_class matches any of the values in the list
	local valid_classes=(0x120000)
	for valid_class in "${valid_classes[@]}"; do
		if [ "$device_class" == "$valid_class" ]; then
			return 0
		fi
	done
	return 1
}

is_bound_to_vfio() {
	local npu=$1
	local existing_driver_name
	local existing_driver

	# return if not bound to any driver
	[ -e "/sys/bus/pci/devices/$npu/driver" ] || return 1

	existing_driver=$(readlink -f "/sys/bus/pci/devices/$npu/driver")
	existing_driver_name=$(basename "$existing_driver")

	[ "$existing_driver_name" == "vfio-pci" ] && return 0
	return 1
}

unbind_device() {
	local npu=$1

	if ! is_target_npu_device $npu; then
		return 0
	fi

	echo "unbinding device $npu"
	unbind_from_driver $npu
}

unbind_all() {
	for dev in /sys/bus/pci/devices/*; do
		read vendor < $dev/vendor
		if [ "$vendor" = "0x1eff" ]; then
			local dev_id=$(basename $dev)
			unbind_device $dev_id
		fi
	done
}

bind_pci_device() {
	local npu=$1

	if ! is_bound_to_vfio $npu; then
		unbind_from_other_driver $npu
		echo "binding device $npu"
		echo "vfio-pci" > /sys/bus/pci/devices/$npu/driver_override
		echo "$npu" > /sys/bus/pci/drivers/vfio-pci/bind
	else
		echo "device $npu already bound to vfio-pci"
	fi
}

bind_device() {
	local npu=$1

	if ! is_target_npu_device $npu; then
		echo "device $npu is not a npu!"
		return 0
	fi

	bind_pci_device "$npu"
}

bind_all() {
	for dev in /sys/bus/pci/devices/*; do
		read vendor < $dev/vendor
		if [ "$vendor" = "0x1eff" ]; then
			local dev_id=$(basename $dev)
			bind_device $dev_id
		fi
	done
}

check_bind_device() {
    local npu=$1

    if ! is_target_npu_device "$npu"; then
        echo "device $npu is not a Rebellions NPU!"
        return 0
    fi

    if is_bound_to_vfio "$npu"; then
        echo "Device $npu is bound to vfio-pci"
        return 0
    else
        echo "Device $npu is not bound to vfio-pci"
        return 1
    fi
}

check_bind_all() {
    local VENDOR_ID="1eff"
    local DEVICE_COUNT=0
    local ALL_BOUND=1

    # Count Rebellions NPU devices
    for dev in /sys/bus/pci/devices/*; do
        if [ -f "$dev/vendor" ]; then
            vendor=$(cat "$dev/vendor")
            dev_id=$(basename "$dev")
            if [ "$vendor" = "0x$VENDOR_ID" ] && is_target_npu_device "$dev_id"; then
                DEVICE_COUNT=$((DEVICE_COUNT + 1))
            fi
        fi
    done

    # If no NPU devices found, return success
    if [ $DEVICE_COUNT -eq 0 ]; then
        echo "No Rebellions NPU devices found."
        return 0
    fi

    # Check if all NPU devices are bound to vfio-pci
    for dev in /sys/bus/pci/devices/*; do
        if [ -f "$dev/vendor" ]; then
            vendor=$(cat "$dev/vendor")
            dev_id=$(basename "$dev")
            if [ "$vendor" = "0x$VENDOR_ID" ] && is_target_npu_device "$dev_id"; then
                if ! is_bound_to_vfio "$dev_id"; then
                    ALL_BOUND=0
                    echo "Device $dev_id is not bound to vfio-pci"
                else
                    echo "Device $dev_id is bound to vfio-pci"
                fi
            fi
        fi
    done

    if [ $ALL_BOUND -eq 1 ]; then
        echo "All $DEVICE_COUNT Rebellions NPU devices are bound to VFIO-PCI."
        return 0
    else
        return 1
    fi
}

handle_bind() {
	chroot /host modprobe vfio-pci
	if [ "$DEVICE_ID" != "" ]; then
		bind_device $DEVICE_ID
	elif [ "$ALL_DEVICES" = "true" ]; then
		bind_all
	else
		usage
	fi
}

handle_unbind() {
	if [ "$DEVICE_ID" != "" ]; then
		unbind_device $DEVICE_ID
	elif [ "$ALL_DEVICES" = "true" ]; then
		unbind_all
	else
		usage
	fi
}

handle_check_bind() {
    if [ "$DEVICE_ID" != "" ]; then
        check_bind_device $DEVICE_ID
    elif [ "$ALL_DEVICES" = "true" ]; then
        check_bind_all
    else
        usage
    fi
}

# ---------- entry ----------
[[ $# -gt 0 ]] || usage

command="$1"
shift || true

DEVICE_ID=""
ALL_DEVICES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -a|--all)
      ALL_DEVICES=true
      shift
      ;;
    -d|--device-id)
      if [[ $# -lt 2 ]]; then
        echo "Error: missing value for $1" >&2
        exit 2
      fi
      DEVICE_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    --) # end of options
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage
      ;;
    *)
      # unexpected positional argument
      echo "Unexpected argument: $1" >&2
      usage
      ;;
  esac
done

case "$command" in
  help)        usage ;;
  bind)        handle_bind ;;
  unbind)      handle_unbind ;;
  check_bind)  handle_check_bind ;;
  *)
    echo "Unknown command: $command" >&2
    usage
    ;;
esac`,
		}
		return ctrl.SetControllerReference(cp, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager ConfigMap")
		return err
	}
	h.log.Info("Reconciled VFIOManager ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", res)
	return nil
}

func (h *vfioManagerPatcher) buildPodSpec() *corev1.PodSpec {
	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.vfio-manager": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(h.desiredSpec.PriorityClassName).
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
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithCommands([]string{"/bin/bash", "-c"}).
				WithArgs([]string{"/bin/vfio-manage.sh bind --all && sleep inf"}).
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
				WithLifeCycle(&corev1.Lifecycle{
					PreStop: &corev1.LifecycleHandler{
						Exec: &corev1.ExecAction{
							Command: []string{"/bin/sh", "-c", "/bin/vfio-manage.sh unbind --all"},
						},
					},
				}).
				Build(),
		}).
		Build()
}

func (h *vfioManagerPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec()

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile VFIOManager DaemonSet")
		return err
	}
	h.log.Info("Reconciled VFIOManager DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
