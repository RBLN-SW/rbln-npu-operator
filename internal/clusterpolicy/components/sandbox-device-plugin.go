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

// sandboxResourceAlias is injected as RBLN_PT_ALIAS into the plugin binary.
// Empty (default) → per-model mode (rebellions.ai/RBLN-<MODEL>_<PF|VF>),
// non-empty → single-alias mode (rebellions.ai/<alias>).
const sandboxResourceAlias = ""

type sandboxDevicePluginPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNSandboxDevicePluginSpec
}

func NewSandboxDevicePluginPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.SandboxDevicePlugin)
	return &sandboxDevicePluginPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNSandboxDevicePluginName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigVMPassthrough,
		},
		desiredSpec: &synced,
	}
}

// Idempotent — backward-compat cleanup
func (h *sandboxDevicePluginPatcher) deleteLegacyConfigMap(ctx context.Context) error {
	return h.deleteIfExists(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name + "-config",
			Namespace: h.namespace,
		},
	})
}

func (h *sandboxDevicePluginPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return h.CleanUp(ctx, owner)
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.deleteLegacyConfigMap(ctx); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *sandboxDevicePluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "Sandbox Device Plugin")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteLegacyConfigMap(ctx); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

// buildVolumes returns the mounts the alias-only plugin pod needs:
//   - validations shared dir (init container output handoff)
//   - kubelet socket dir (register/serve)
//   - /dev/vfio (hand VFIO char devices to virt-launcher)
//   - /sys/bus/pci/devices RO (sysfs scan)
//   - /sys (vfio-pci-validation init container reads driver symlinks)
func (h *sandboxDevicePluginPatcher) buildVolumes() []corev1.Volume {
	return []corev1.Volume{
		hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
		hostPathVolume("devicesock", "/var/lib/kubelet/device-plugins", corev1.HostPathDirectory),
		hostPathVolume("host-vfio", "/dev/vfio", corev1.HostPathDirectory),
		hostPathVolume("sys-bus-pci", "/sys/bus/pci/devices", corev1.HostPathDirectory),
		hostPathVolume("host-sys", "/sys", corev1.HostPathDirectory),
	}
}

func (h *sandboxDevicePluginPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildVFIOPCIValidationInitContainer(owner.Spec.Validator)

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.sandbox-device-plugin": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithVolumes(h.buildVolumes()).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithResources(h.desiredSpec.Resources, "250m", "40Mi").
				WithEnvs([]corev1.EnvVar{
					// RBLN_PT_ALIAS empty → per-model; non-empty → single-alias.
					{Name: "RBLN_PT_ALIAS", Value: sandboxResourceAlias},
					// Next two match binary defaults; spelled out for review visibility.
					{Name: "RBLN_SYSFS_PCI_PATH", Value: "/sys/bus/pci/devices"},
					{Name: "RBLN_KUBELET_DEVICE_PLUGIN_PATH", Value: "/var/lib/kubelet/device-plugins"},
				}).
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: "devicesock", MountPath: "/var/lib/kubelet/device-plugins"},
					{Name: "host-vfio", MountPath: "/dev/vfio"},
					{Name: "sys-bus-pci", MountPath: "/sys/bus/pci/devices", ReadOnly: true},
				}).
				WithSecurityContext(&corev1.SecurityContext{
					Privileged: ptr(true),
					RunAsUser:  ptr(int64(0)),
				}).
				Build(),
		}).
		Build()
}

func (h *sandboxDevicePluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	sel := selectorLabels(consts.RBLNSandboxDevicePluginName)

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(sel).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile SandboxDevicePlugin DaemonSet")
		return err
	}
	h.log.Info("Reconciled SandboxDevicePlugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
