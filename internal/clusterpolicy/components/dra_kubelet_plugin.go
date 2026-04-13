package components

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const (
	draCDIRootPath       = "/var/run/cdi"
	draHostRunRBLNPath   = "/run/rbln"
	draHostUsrBinPath    = "/usr/bin"
	draHostUsrBinMount   = "/host/usr/bin"
	draClusterRoleSuffix = "-role"
	draClusterBindSuffix = "-rolebinding"
	draExtendedResource  = "rebellions.ai/npu"
)

type draKubeletPluginPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNDRAKubeletPluginSpec
	podDefaults *rblnv1beta1.PodDefaultsSpec
}

func NewDRAKubeletPluginPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.DRAKubeletPlugin)
	return &draKubeletPluginPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec: &synced,
		podDefaults: cpSpec.PodDefaults,
	}
}

func (h *draKubeletPluginPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if !owner.Spec.ContainerToolkit.IsEnabled() {
		return fmt.Errorf("DRA kubelet plugin requires containerToolkit to be enabled")
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.handleDRAClass(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *draKubeletPluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "DRA kubelet plugin")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteDeviceClass(ctx); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: h.clusterRoleBindingName()},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: h.clusterRoleName()},
	}); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *draKubeletPluginPatcher) clusterRoleName() string {
	return h.name + draClusterRoleSuffix
}

func (h *draKubeletPluginPatcher) clusterRoleBindingName() string {
	return h.name + draClusterBindSuffix
}

func (h *draKubeletPluginPatcher) className() string {
	if h.desiredSpec != nil && h.desiredSpec.DriverName != "" {
		return h.desiredSpec.DriverName
	}
	return "npu.rebellions.ai"
}

func (h *draKubeletPluginPatcher) handleDRAClass(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	return h.handleDeviceClass(ctx, owner)
}

func (h *draKubeletPluginPatcher) handleDeviceClass(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	deviceClass := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: h.className()},
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, deviceClass, func() error {
		deviceClass.Spec = resourcev1.DeviceClassSpec{
			Selectors: []resourcev1.DeviceSelector{
				{
					CEL: &resourcev1.CELDeviceSelector{
						Expression: fmt.Sprintf("device.driver == %q", h.className()),
					},
				},
			},
			ExtendedResourceName: ptr(draExtendedResource),
		}
		return ctrl.SetControllerReference(owner, deviceClass, h.scheme)
	})
	if err != nil {
		return err
	}
	h.log.Info("Reconciled DRA DeviceClass", "name", deviceClass.Name, "result", res)
	return nil
}

func (h *draKubeletPluginPatcher) deleteDeviceClass(ctx context.Context) error {
	if err := h.client.Delete(ctx, &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: h.className()},
	}); err != nil && !kapierrors.IsNotFound(err) && !isNoMatchError(err) {
		return err
	}
	return nil
}

func isNoMatchError(err error) bool {
	if err == nil {
		return false
	}
	return apimeta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) || strings.Contains(err.Error(), "no matches for kind")
}

func (h *draKubeletPluginPatcher) handleClusterRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	clusterRole := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.clusterRoleName()}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, clusterRole, func() error {
		clusterRole.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{"resource.k8s.io"},
				Resources: []string{"resourceclaims"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{"resource.k8s.io"},
				Resources: []string{"deviceclasses", "resourceclasses"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{"resource.k8s.io"},
				Resources: []string{"resourceslices"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		}
		return ctrl.SetControllerReference(owner, clusterRole, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin ClusterRole")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin ClusterRole", "name", clusterRole.Name, "result", res)
	return nil
}

func (h *draKubeletPluginPatcher) handleClusterRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.clusterRoleBindingName()}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     h.clusterRoleName(),
		}
		binding.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.name,
				Namespace: h.namespace,
			},
		}
		return ctrl.SetControllerReference(owner, binding, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin ClusterRoleBinding")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin ClusterRoleBinding", "name", binding.Name, "result", res)
	return nil
}

func (h *draKubeletPluginPatcher) buildDRAContainer() *corev1.Container {
	env := []corev1.EnvVar{
		{Name: "DRIVER_NAME", Value: h.desiredSpec.DriverName},
		{Name: "CDI_ROOT", Value: draCDIRootPath},
		{Name: "KUBELET_REGISTRAR_DIRECTORY_PATH", Value: h.desiredSpec.KubeletRegistrarDirectoryPath},
		{Name: "KUBELET_PLUGINS_DIRECTORY_PATH", Value: h.desiredSpec.KubeletPluginsDirectoryPath},
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "spec.nodeName",
			}},
		},
		{
			Name: "NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "metadata.namespace",
			}},
		},
	}

	var livenessProbe *corev1.Probe
	if h.desiredSpec.HealthcheckPort > 0 {
		env = append(env, corev1.EnvVar{
			Name:  "HEALTHCHECK_PORT",
			Value: fmt.Sprintf("%d", h.desiredSpec.HealthcheckPort),
		})
		livenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				GRPC: &corev1.GRPCAction{
					Port:    h.desiredSpec.HealthcheckPort,
					Service: ptr("liveness"),
				},
			},
			FailureThreshold: 3,
			PeriodSeconds:    10,
		}
	}

	return k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithCommands([]string{"npu-kubelet-plugin"}).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithSecurityContext(&corev1.SecurityContext{Privileged: ptr(true)}).
		WithEnvs(env).
		WithLivenessProbe(livenessProbe).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
			{Name: "plugins-registry", MountPath: h.desiredSpec.KubeletRegistrarDirectoryPath},
			{Name: "plugins", MountPath: h.desiredSpec.KubeletPluginsDirectoryPath},
			{Name: "cdi", MountPath: draCDIRootPath},
			{Name: "host-dev", MountPath: "/dev"},
			{Name: "host-run-rbln", MountPath: draHostRunRBLNPath},
			{Name: "host-usr-bin", MountPath: draHostUsrBinMount, ReadOnly: true},
		}).
		Build()
}

func (h *draKubeletPluginPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildToolkitValidationInitContainer(owner.Spec.Validator)
	draContainer := h.buildDRAContainer()

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.dra-kubelet-plugin": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(podDefaultsPriorityClassName(h.podDefaults)).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			{
				Name: "plugins-registry",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: h.desiredSpec.KubeletRegistrarDirectoryPath, Type: ptr(corev1.HostPathDirectoryOrCreate),
				}},
			},
			{
				Name: "plugins",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: h.desiredSpec.KubeletPluginsDirectoryPath, Type: ptr(corev1.HostPathDirectoryOrCreate),
				}},
			},
			hostPathVolume("cdi", draCDIRootPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("host-dev", "/dev", corev1.HostPathDirectory),
			hostPathVolume("host-run-rbln", draHostRunRBLNPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("host-usr-bin", draHostUsrBinPath, corev1.HostPathDirectory),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{draContainer}).
		Build()
}

func (h *draKubeletPluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	sel := selectorLabels(consts.RBLNDRAKubeletPluginName)

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(sel).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin DaemonSet")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
