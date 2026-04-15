package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const (
	hostUsrBinVolumeName      = "host-usr-bin"
	hostUsrBinPath            = "/usr/bin"
	hostUsrBinMountPath       = "/host/usr/bin"
	hostDriverUsrBinPath      = "/run/rbln/driver/usr/bin"
	hostDriverUsrBinName      = "host-driver-usr-bin"
	hostDriverUsrBinMountPath = "/host/driver/usr/bin"
)

type devicePluginPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNDevicePluginSpec
}

func NewDevicePluginPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.DevicePlugin)
	return &devicePluginPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec: &synced,
	}
}

func (h *devicePluginPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *devicePluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "Device Plugin")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *devicePluginPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildToolkitValidationInitContainer(owner.Spec.Validator)

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.device-plugin": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("devicesock", "/var/lib/kubelet/device-plugins", corev1.HostPathDirectory),
			hostPathVolume("plugins-registry", "/var/lib/kubelet/plugins_registry", corev1.HostPathDirectory),
			hostPathVolume("log", "/var/log", corev1.HostPathDirectory),
			{
				Name: "device-info",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/run/k8s.cni.cncf.io/devinfo/dp",
					Type: ptr(corev1.HostPathDirectoryOrCreate),
				}},
			},
			hostPathVolume("host-sys", "/sys", corev1.HostPathDirectory),
			hostPathVolume("host-dev", "/dev", corev1.HostPathDirectory),
			hostPathVolume(hostUsrBinVolumeName, hostUsrBinPath, corev1.HostPathDirectory),
			hostPathVolume(hostDriverUsrBinName, hostDriverUsrBinPath, corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithResources(h.desiredSpec.Resources, "250m", "40Mi").
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: "devicesock", MountPath: "/var/lib/kubelet/device-plugins"},
					{Name: "plugins-registry", MountPath: "/var/lib/kubelet/plugins_registry"},
					{Name: "log", MountPath: "/var/log"},
					{Name: "device-info", MountPath: "/var/run/k8s.cni.cncf.io/devinfo/dp"},
					{Name: hostUsrBinVolumeName, MountPath: hostUsrBinMountPath, ReadOnly: true},
					{Name: hostDriverUsrBinName, MountPath: hostDriverUsrBinMountPath, ReadOnly: true},
					{Name: "host-dev", MountPath: "/dev"},
					{Name: "host-sys", MountPath: "/sys"},
				}).
				WithSecurityContext(&corev1.SecurityContext{
					Privileged: ptr(true),
					RunAsUser:  ptr(int64(0)),
				}).
				Build(),
		}).
		Build()
}

func (h *devicePluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

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
		h.log.Error(err, "Failed to reconcile DevicePlugin DaemonSet")
		return err
	}
	h.log.Info("Reconciled DevicePlugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
