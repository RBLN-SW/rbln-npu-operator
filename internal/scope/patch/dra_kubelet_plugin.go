package patch

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.RBLNDRAKubeletPluginSpec
	name             string
	namespace        string
	openshiftVersion string
}

func NewDRAKubeletPluginPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) (Patcher, error) {
	patcher := &draKubeletPluginPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             cpSpec.BaseName + "-" + consts.RBLNDRAKubeletPluginName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
	}

	synced := syncSpec(cpSpec, cpSpec.DRAKubeletPlugin)
	patcher.desiredSpec = &synced
	return patcher, nil
}

func (h *draKubeletPluginPatcher) IsEnabled() bool {
	if h.desiredSpec == nil {
		return false
	}
	return h.desiredSpec.IsEnabled()
}

func (h *draKubeletPluginPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}

	if !owner.Spec.ContainerToolkit.IsEnabled() {
		return fmt.Errorf("DRA kubelet plugin requires containerToolkit to be enabled")
	}

	if err := h.handleServiceAccount(ctx, owner); err != nil {
		return err
	}

	if h.openshiftVersion != "" {
		if err := h.handleRole(ctx, owner); err != nil {
			return err
		}
		if err := h.handleRoleBinding(ctx, owner); err != nil {
			return err
		}
	}

	if err := h.handleClusterRole(ctx); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx); err != nil {
		return err
	}
	if err := h.handleDRAClass(ctx); err != nil {
		return err
	}
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}

	return nil
}

func (h *draKubeletPluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: DRA kubelet plugin is disabled. Remove all DRA kubelet plugin resources")

	if err := h.client.Delete(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.deleteDeviceClass(ctx); err != nil {
		return err
	}

	if err := h.client.Delete(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.clusterRoleBindingName(),
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.client.Delete(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.clusterRoleName(),
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.client.Delete(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.client.Delete(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.client.Delete(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func (h *draKubeletPluginPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
	var ds appsv1.DaemonSet
	if err := h.client.Get(ctx, types.NamespacedName{Name: h.name, Namespace: h.namespace}, &ds); err != nil {
		return []metav1.Condition{{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionFalse,
			Reason:             DaemonSetNotFound,
			Message:            fmt.Sprintf("DaemonSet %s/%s could not be found: %v", h.namespace, h.name, err),
			LastTransitionTime: metav1.Now(),
		}}, nil
	}

	ready := ds.Status.DesiredNumberScheduled > 0 &&
		ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberUnavailable == 0

	observedGen := ds.GetGeneration()
	if !ready {
		return []metav1.Condition{
			{
				Type:   DaemonSetReady,
				Status: metav1.ConditionFalse,
				Reason: DaemonSetPodsNotReady,
				Message: fmt.Sprintf(
					"DaemonSet %s/%s is progressing: %d of %d pods are Ready (%d unavailable)",
					h.namespace,
					h.name,
					ds.Status.NumberReady,
					ds.Status.DesiredNumberScheduled,
					ds.Status.NumberUnavailable,
				),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: observedGen,
			},
		}, nil
	}

	return []metav1.Condition{
		{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionTrue,
			Reason:             DaemonSetAllPodsReady,
			Message:            fmt.Sprintf("All pods in DaemonSet %s/%s are running", h.namespace, h.name),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: observedGen,
		},
	}, nil
}

func (h *draKubeletPluginPatcher) ComponentName() string {
	return h.name
}

func (h *draKubeletPluginPatcher) ComponentNamespace() string {
	return h.namespace
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

func (h *draKubeletPluginPatcher) handleDRAClass(ctx context.Context) error {
	return h.handleDeviceClass(ctx)
}

func (h *draKubeletPluginPatcher) handleDeviceClass(ctx context.Context) error {
	deviceClass := &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.className(),
		},
	}

	classRes, err := controllerutil.CreateOrPatch(ctx, h.client, deviceClass, func() error {
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
		return nil
	})
	if err != nil {
		return err
	}

	h.log.Info("Reconciled DRA DeviceClass", "name", deviceClass.Name, "apiVersion", "resource.k8s.io/v1", "result", classRes)
	return nil
}

func (h *draKubeletPluginPatcher) deleteDeviceClass(ctx context.Context) error {
	if err := h.client.Delete(ctx, &resourcev1.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.className(),
		},
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

func (h *draKubeletPluginPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin ServiceAccount")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *draKubeletPluginPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBuilder(h.name, h.namespace)
	role := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role = builder.
			WithRules(rbacv1.PolicyRule{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			}).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin Role")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin Role", "namespace", role.Namespace, "name", role.Name, "result", roleRes)
	return nil
}

func (h *draKubeletPluginPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBindingBuilder(h.name, h.namespace)
	roleBinding := builder.Build()

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, roleBinding, func() error {
		roleBinding = builder.
			WithRoleRef(rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     h.name,
			}).
			WithSubjects(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.name,
				Namespace: h.namespace,
			}).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin RoleBinding")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin RoleBinding", "namespace", roleBinding.Namespace, "name", roleBinding.Name, "result", bindingRes)
	return nil
}

func (h *draKubeletPluginPatcher) handleClusterRole(ctx context.Context) error {
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.clusterRoleName(),
		},
	}

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, clusterRole, func() error {
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
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin ClusterRole")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin ClusterRole", "name", clusterRole.Name, "result", roleRes)
	return nil
}

func (h *draKubeletPluginPatcher) handleClusterRoleBinding(ctx context.Context) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.clusterRoleBindingName(),
		},
	}

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
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
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin ClusterRoleBinding")
		return err
	}
	h.log.Info("Reconciled DRA kubelet plugin ClusterRoleBinding", "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *draKubeletPluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	validatorSpec := owner.Spec.Validator
	toolkitValidationInit := k8sutil.NewContainerBuilder().
		WithName("toolkit-validation").
		WithImage(ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{"sh", "-c"}).
		WithArgs([]string{"until [ -f " + validationsMountPath + "/toolkit-ready ]; do echo waiting for rbln container stack to be setup; sleep 5; done"}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             validationsVolumeName,
				MountPath:        validationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
	if validatorSpec.ImagePullPolicy == "" {
		toolkitValidationInit.ImagePullPolicy = corev1.PullIfNotPresent
	}

	draContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithCommands([]string{"npu-kubelet-plugin"}).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
		}).
		WithEnvs([]corev1.EnvVar{
			{
				Name:  "DRIVER_NAME",
				Value: h.desiredSpec.DriverName,
			},
			{
				Name:  "CDI_ROOT",
				Value: draCDIRootPath,
			},
			{
				Name:  "KUBELET_REGISTRAR_DIRECTORY_PATH",
				Value: h.desiredSpec.KubeletRegistrarDirectoryPath,
			},
			{
				Name:  "KUBELET_PLUGINS_DIRECTORY_PATH",
				Value: h.desiredSpec.KubeletPluginsDirectoryPath,
			},
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: "NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:      validationsVolumeName,
				MountPath: validationsMountPath,
			},
			{
				Name:      "plugins-registry",
				MountPath: h.desiredSpec.KubeletRegistrarDirectoryPath,
			},
			{
				Name:      "plugins",
				MountPath: h.desiredSpec.KubeletPluginsDirectoryPath,
			},
			{
				Name:      "cdi",
				MountPath: draCDIRootPath,
			},
			{
				Name:      "host-dev",
				MountPath: "/dev",
			},
			{
				Name:      "host-run-rbln",
				MountPath: draHostRunRBLNPath,
			},
			{
				Name:      "host-usr-bin",
				MountPath: draHostUsrBinMount,
				ReadOnly:  true,
			},
		}).
		Build()

	if h.desiredSpec.HealthcheckPort > 0 {
		draContainer.Env = append(draContainer.Env, corev1.EnvVar{
			Name:  "HEALTHCHECK_PORT",
			Value: fmt.Sprintf("%d", h.desiredSpec.HealthcheckPort),
		})
		draContainer.LivenessProbe = &corev1.Probe{
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

	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(k8sutil.NewPodSpecBuilder().
				WithServiceAccountName(h.name).
				WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.dra-kubelet-plugin": "true"}).
				WithAffinity(h.desiredSpec.Affinity).
				WithTolerations(h.desiredSpec.Tolerations).
				WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
				WithPriorityClassName(h.desiredSpec.PriorityClassName).
				WithVolumes([]corev1.Volume{
					{
						Name: validationsVolumeName,
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: validationsMountPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: "plugins-registry",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: h.desiredSpec.KubeletRegistrarDirectoryPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: "plugins",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: h.desiredSpec.KubeletPluginsDirectoryPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: "cdi",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: draCDIRootPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: "host-dev",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/dev",
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
					{
						Name: "host-run-rbln",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: draHostRunRBLNPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: "host-usr-bin",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: draHostUsrBinPath,
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
				}).
				WithInitContainers([]*corev1.Container{toolkitValidationInit}).
				WithContainers([]*corev1.Container{draContainer}).
				Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile DRA kubelet plugin DaemonSet")
		return err
	}

	h.log.Info("Reconciled DRA kubelet plugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}
