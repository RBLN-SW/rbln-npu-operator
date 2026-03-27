package components

import (
	"context"
	"encoding/json"

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
			name:             cpSpec.BaseName + "-" + consts.RBLNSandboxDevicePluginName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec: &synced,
	}
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
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *sandboxDevicePluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Sandbox Device Plugin is disabled. Remove all Sandbox Device Plugin resources")
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

func (h *sandboxDevicePluginPatcher) buildSandboxDevicePluginConfig() (string, error) {
	configResources := make([]configResource, 0)
	for _, resource := range h.desiredSpec.ResourceList {
		devices, err := collectDevices(resource.ProductCardNames)
		if err != nil {
			h.log.Error(err, "Failed to collect devices for resource", "resourceName", resource.ResourceName)
			return "", err
		}
		configResources = append(configResources, configResource{
			ResourceName:   resource.ResourceName,
			ResourcePrefix: resource.ResourcePrefix,
			DeviceType:     consts.DeviceTypeAccelerator,
			Selectors: deviceSelector{
				Vendors: []string{consts.RBLNVendorCode},
				Drivers: []string{"vfio-pci"},
				Devices: devices,
			},
		})
	}
	configDataBytes, err := json.MarshalIndent(configResourceList{ResourceList: configResources}, "", "  ")
	if err != nil {
		h.log.Error(err, "Failed to marshal sandbox device plugin config")
		return "", err
	}
	return string(configDataBytes), nil
}

func (h *sandboxDevicePluginPatcher) handleConfigMap(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace},
	}

	configData, err := h.buildSandboxDevicePluginConfig()
	if err != nil {
		return err
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = map[string]string{"config.json": configData}
		return ctrl.SetControllerReference(cp, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile SandboxDevicePlugin ConfigMap")
		return err
	}
	h.log.Info("Reconciled SandboxDevicePlugin ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", res)
	return nil
}

func (h *sandboxDevicePluginPatcher) buildVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "devicesock",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/var/lib/kubelet/device-plugins",
			}},
		},
		{
			Name: "plugins-registry",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/var/lib/kubelet/plugins_registry",
			}},
		},
		{
			Name: "log",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: "/var/log",
			}},
		},
		{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: h.name + "-config"},
				Items:                []corev1.KeyToPath{{Key: "config.json", Path: "config.json"}},
			}},
		},
		hostPathVolume("host-sys", "/sys", corev1.HostPathDirectory),
		hostPathVolume("host-dev", "/dev", corev1.HostPathDirectory),
		hostPathVolume("host-vfio", "/dev/vfio", corev1.HostPathDirectory),
	}
}

func (h *sandboxDevicePluginPatcher) buildPodSpec() *corev1.PodSpec {
	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.sandbox-device-plugin": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithVolumes(h.buildVolumes()).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithResources(h.desiredSpec.Resources, "250m", "40Mi").
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: "devicesock", MountPath: "/var/lib/kubelet/device-plugins"},
					{Name: "plugins-registry", MountPath: "/var/lib/kubelet/plugins_registry"},
					{Name: "log", MountPath: "/var/log"},
					{Name: "config-volume", MountPath: "/etc/pcidp"},
					{Name: "host-dev", MountPath: "/dev"},
					{Name: "host-vfio", MountPath: "/dev/vfio"},
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

func (h *sandboxDevicePluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile SandboxDevicePlugin DaemonSet")
		return err
	}
	h.log.Info("Reconciled SandboxDevicePlugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
