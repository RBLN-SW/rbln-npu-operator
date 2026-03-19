package components

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
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
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.ValidatorSpec
	name             string
	namespace        string
	openshiftVersion string
	daemonsets       *rblnv1beta1.DaemonsetsSpec
}

func NewValidatorPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	patcher := &validatorPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             cpSpec.BaseName + "-" + consts.RBLNValidatorName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
		daemonsets:       cpSpec.Daemonsets,
	}

	patcher.desiredSpec = &cpSpec.Validator
	return patcher
}

func (h *validatorPatcher) IsEnabled() bool {
	return h.desiredSpec != nil
}

func (h *validatorPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}

	if err := h.handleServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.handleRole(ctx, owner); err != nil {
		return err
	}
	if err := h.handleRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx); err != nil {
		return err
	}
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}
	return nil
}

func (h *validatorPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Validator is disabled. Remove all Validator resources")

	if err := h.client.Delete(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
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

func (h *validatorPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
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

func (h *validatorPatcher) ComponentName() string {
	return h.name
}

func (h *validatorPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *validatorPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator ServiceAccount")
		return err
	}
	h.log.Info("Reconciled Validator ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *validatorPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBuilder(h.name, h.namespace)
	role := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
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

		role = builder.
			WithRules(rules...).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator Role")
		return err
	}
	h.log.Info("Reconciled Validator Role", "namespace", role.Namespace, "name", role.Name, "result", roleRes)
	return nil
}

func (h *validatorPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBindingBuilder(h.name, h.namespace)
	binding := builder.Build()

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding = builder.
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
		h.log.Error(err, "Failed to reconcile Validator RoleBinding")
		return err
	}
	h.log.Info("Reconciled Validator RoleBinding", "namespace", binding.Namespace, "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *validatorPatcher) handleClusterRole(ctx context.Context) error {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
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
	h.log.Info("Reconciled Validator ClusterRole", "name", role.Name, "result", roleRes)
	return nil
}

func (h *validatorPatcher) handleClusterRoleBinding(ctx context.Context) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
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
	h.log.Info("Reconciled Validator ClusterRoleBinding", "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *validatorPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	validatorSpec := h.desiredSpec
	imagePullPolicy := validatorSpec.ImagePullPolicy
	if imagePullPolicy == "" {
		imagePullPolicy = corev1.PullIfNotPresent
	}

	validatorImage := ComposeImageReference(validatorSpec.Registry, validatorSpec.Image)

	driverArgs := validatorComponentArgs(validatorSpec.Args, validatorComponentDriver)
	toolkitArgs := validatorComponentArgs(validatorSpec.Args, validatorComponentToolkit)
	baseEnv := mergeEnvVars(
		[]corev1.EnvVar{
			{
				Name:  "TMPDIR",
				Value: validationsMountPath,
			},
		},
		validatorSpec.Env,
	)

	driverEnv := mergeEnvVars(
		baseEnv,
		validatorSpec.Driver.Env,
	)
	toolkitEnv := mergeEnvVars(
		baseEnv,
		validatorSpec.Toolkit.Env,
	)
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
			{
				Name:      validatorCDIRootVolumeName,
				MountPath: validatorCDIRootPath,
			},
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
	mainContainer := mainContainerBuilder.Build()

	labels := map[string]string{"app": h.name}
	annotations := map[string]string(nil)
	affinity := (*corev1.Affinity)(nil)
	tolerations := []corev1.Toleration(nil)
	priorityClassName := ""
	if h.daemonsets != nil {
		labels = k8sutil.MergeMaps(labels, h.daemonsets.Labels)
		annotations = h.daemonsets.Annotations
		affinity = h.daemonsets.Affinity
		tolerations = h.daemonsets.Tolerations
		priorityClassName = h.daemonsets.PriorityClassName
	}

	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(labels).
			WithAnnotations(annotations).
			WithPodSpec(k8sutil.NewPodSpecBuilder().
				WithServiceAccountName(h.name).
				WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.operator-validator": "true"}).
				WithAffinity(affinity).
				WithTolerations(tolerations).
				WithImagePullSecrets(validatorSpec.ImagePullSecrets).
				WithPriorityClassName(priorityClassName).
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
						Name: validatorHostDriverVolumeName,
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: validatorHostDriverPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
					{
						Name: validatorHostRootVolumeName,
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: validatorHostRootPath,
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
					{
						Name: validatorCDIRootVolumeName,
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: validatorCDIRootPath,
								Type: ptr(corev1.HostPathDirectoryOrCreate),
							},
						},
					},
				}).
				WithInitContainers([]*corev1.Container{
					driverInit,
					toolkitInit,
				}).
				WithContainers([]*corev1.Container{
					mainContainer,
				}).
				Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Validator DaemonSet")
		return err
	}

	h.log.Info("Reconciled Validator DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}

func validatorComponentArgs(baseArgs []string, component string) []string {
	args := []string{component}
	if len(baseArgs) > 0 {
		args = append(args, baseArgs...)
	}
	return args
}

func mergeEnvVars(base []corev1.EnvVar, additions ...[]corev1.EnvVar) []corev1.EnvVar {
	merged := slices.Clone(base)
	for _, list := range additions {
		for _, env := range list {
			replaced := false
			for idx := range merged {
				if merged[idx].Name == env.Name {
					merged[idx] = env
					replaced = true
					break
				}
			}
			if !replaced {
				merged = append(merged, env)
			}
		}
	}
	return merged
}
