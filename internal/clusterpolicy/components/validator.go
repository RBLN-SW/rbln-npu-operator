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

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const (
	validatorComponentDriver  = "driver"
	validatorComponentToolkit = "toolkit"

	validatorDefaultCommand = "rbln-validator"

	validatorHostRootVolumeName   = "host-root"
	validatorHostRootPath         = "/"
	validatorHostDriverVolumeName = "driver-install-dir"
	validatorHostDriverPath       = "/run/rbln/driver"
	validatorCDIRootVolumeName    = "cdi-root"
	validatorCDIRootPath          = "/var/run/cdi"
)

type validatorPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.ValidatorSpec
	daemonsets  *rblnv1beta1.DaemonsetsSpec
}

func NewValidatorPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	return &validatorPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             cpSpec.BaseName + "-" + consts.RBLNValidatorName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          true,
		},
		desiredSpec: &cpSpec.Validator,
		daemonsets:  cpSpec.Daemonsets,
	}
}

func (h *validatorPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	// Validator always needs RBAC (pods + daemonsets access), not just on OpenShift.
	if err := h.reconcileValidatorRole(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *validatorPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Validator is disabled. Remove all Validator resources")
	if err := h.deleteDaemonSet(ctx); err != nil {
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
	return h.deleteServiceAccount(ctx)
}

// reconcileValidatorRole creates a Role with pods + daemonsets access, and
// additionally grants OpenShift SCC privileges when running on OpenShift.
func (h *validatorPatcher) reconcileValidatorRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		rules := []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"create", "get", "list", "watch", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"get", "list", "watch"},
			},
		}
		if h.openshiftVersion != "" {
			rules = append(rules, rbacv1.PolicyRule{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			})
		}
		role.Rules = rules
		return ctrl.SetControllerReference(owner, role, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator Role", "name", h.name)
		return err
	}
	h.log.Info("Reconciled Validator Role", "name", role.Name, "namespace", role.Namespace, "result", res)
	return nil
}

func (h *validatorPatcher) handleClusterRole(ctx context.Context) error {
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.name}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
		}
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator ClusterRole")
		return err
	}
	h.log.Info("Reconciled Validator ClusterRole", "name", role.Name, "result", res)
	return nil
}

func (h *validatorPatcher) handleClusterRoleBinding(ctx context.Context) error {
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     h.name,
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
		h.log.Error(err, "Failed to reconcile Validator ClusterRoleBinding")
		return err
	}
	h.log.Info("Reconciled Validator ClusterRoleBinding", "name", binding.Name, "result", res)
	return nil
}

func (h *validatorPatcher) buildPodSpec() *corev1.PodSpec {
	validatorSpec := h.desiredSpec
	imagePullPolicy := validatorSpec.ImagePullPolicy
	if imagePullPolicy == "" {
		imagePullPolicy = corev1.PullIfNotPresent
	}

	validatorImage := ComposeImageReference(validatorSpec.Registry, validatorSpec.Image)

	driverArgs := validatorComponentArgs(validatorSpec.Args, validatorComponentDriver)
	toolkitArgs := validatorComponentArgs(validatorSpec.Args, validatorComponentToolkit)
	baseEnv := mergeEnvVars(
		[]corev1.EnvVar{{Name: "TMPDIR", Value: validationsMountPath}},
		validatorSpec.Env,
	)
	driverEnv := mergeEnvVars(baseEnv, validatorSpec.Driver.Env)
	toolkitEnv := mergeEnvVars(baseEnv, validatorSpec.Toolkit.Env)

	driverInit := k8sutil.NewContainerBuilder().
		WithName("driver-validation").
		WithImage(validatorImage, validatorSpec.Version, imagePullPolicy).
		WithCommands([]string{validatorDefaultCommand}).
		WithArgs(driverArgs).
		WithEnvs(driverEnv).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             validatorHostRootVolumeName,
				MountPath:        "/host",
				ReadOnly:         true,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
			{
				Name:             validatorHostDriverVolumeName,
				MountPath:        validatorHostDriverPath,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
			{
				Name:             validationsVolumeName,
				MountPath:        validationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationBidirectional),
			},
		}).
		Build()

	toolkitInit := k8sutil.NewContainerBuilder().
		WithName("toolkit-validation").
		WithImage(validatorImage, validatorSpec.Version, imagePullPolicy).
		WithCommands([]string{validatorDefaultCommand}).
		WithArgs(toolkitArgs).
		WithEnvs(toolkitEnv).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             validationsVolumeName,
				MountPath:        validationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationBidirectional),
			},
			{Name: validatorCDIRootVolumeName, MountPath: validatorCDIRootPath},
		}).
		Build()

	mainContainerBuilder := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(validatorImage, validatorSpec.Version, imagePullPolicy).
		WithCommands([]string{"sh", "-c"}).
		WithArgs([]string{"echo all validations are successful; while true; do sleep 86400; done"}).
		WithEnvs(baseEnv).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithLifeCycle(&corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sh", "-c", "rm -f " + validationsMountPath + "/*-ready"},
				},
			},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             validationsVolumeName,
				MountPath:        validationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationBidirectional),
			},
		})
	if validatorSpec.Resources != nil {
		mainContainerBuilder.WithResources(*validatorSpec.Resources, "250m", "40Mi")
	}

	affinity := (*corev1.Affinity)(nil)
	tolerations := []corev1.Toleration(nil)
	priorityClassName := ""
	if h.daemonsets != nil {
		affinity = h.daemonsets.Affinity
		tolerations = h.daemonsets.Tolerations
		priorityClassName = h.daemonsets.PriorityClassName
	}

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.operator-validator": "true"}).
		WithAffinity(affinity).
		WithTolerations(tolerations).
		WithImagePullSecrets(validatorSpec.ImagePullSecrets).
		WithPriorityClassName(priorityClassName).
		WithVolumes([]corev1.Volume{
			hostPathVolume(validationsVolumeName, validationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume(validatorHostDriverVolumeName, validatorHostDriverPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume(validatorHostRootVolumeName, validatorHostRootPath, corev1.HostPathDirectory),
			hostPathVolume(validatorCDIRootVolumeName, validatorCDIRootPath, corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{driverInit, toolkitInit}).
		WithContainers([]*corev1.Container{mainContainerBuilder.Build()}).
		Build()
}

func (h *validatorPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec()

	labels := map[string]string{"app": h.name}
	annotations := map[string]string(nil)
	if h.daemonsets != nil {
		labels = k8sutil.MergeMaps(labels, h.daemonsets.Labels)
		annotations = h.daemonsets.Annotations
	}

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(labels).
			WithAnnotations(annotations).
			WithPodSpec(podSpec)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator DaemonSet")
		return err
	}
	h.log.Info("Reconciled Validator DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}

func validatorComponentArgs(baseArgs []string, component string) []string {
	args := []string{component}
	if len(baseArgs) > 0 {
		args = append(args, baseArgs...)
	}
	return args
}
