package components

import (
	"context"
	"fmt"
	"slices"
	"strings"

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
	containerToolkitCDIRootVolumeName = "cdi-root"
	containerToolkitCDIRootPath       = "/var/run/cdi"
	containerToolkitRunVolumeName     = "run-rbln"
	containerToolkitRunPath           = "/run/rbln"
	containerToolkitHostBinVolumeName = "host-bin"
	containerToolkitHostBinPath       = "/usr/local/bin"
	containerToolkitHostBinMountPath  = "/host/usr/local/bin"
	containerdSockVolumeName          = "containerd-sock"
	containerdSockPath                = "/run/containerd/containerd.sock"
	dockerSockVolumeName              = "docker-sock"
	dockerSockPath                    = "/var/run/docker.sock"
	crioSockVolumeName                = "crio-sock"
	crioSockPath                      = "/var/run/crio/crio.sock"
	containerToolkitEntrypointKey     = "entrypoint.sh"
	containerToolkitEntrypointPath    = "/bin/entrypoint.sh"
)

type containerToolkitPatcher struct {
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.RBLNContainerToolkitSpec
	name             string
	namespace        string
	openshiftVersion string
	containerRuntime string
}

func NewContainerToolkitPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string, containerRuntime string) Patcher {
	patcher := &containerToolkitPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             cpSpec.BaseName + "-" + consts.RBLNContainerToolkitName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
		containerRuntime: containerRuntime,
	}

	synced := syncSpec(cpSpec, cpSpec.ContainerToolkit)
	patcher.desiredSpec = &synced
	return patcher
}

func (h *containerToolkitPatcher) IsEnabled() bool {
	if h.desiredSpec == nil {
		return false
	}
	return h.desiredSpec.IsEnabled()
}

func (h *containerToolkitPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}

	return nil
}

func (h *containerToolkitPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Container Toolkit is disabled. Remove all Container Toolkit resources")

	if err := h.client.Delete(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
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
	if err := h.client.Delete(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.entrypointConfigMapName(),
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

func (h *containerToolkitPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
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

func (h *containerToolkitPatcher) ComponentName() string {
	return h.name
}

func (h *containerToolkitPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *containerToolkitPatcher) entrypointConfigMapName() string {
	return h.name + "-entrypoint"
}

func (h *containerToolkitPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Container Toolkit ServiceAccount")
		return err
	}
	h.log.Info("Reconciled Container Toolkit ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *containerToolkitPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBuilder(h.name, h.namespace)
	role := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		rules := []rbacv1.PolicyRule{
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"list"},
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
		h.log.Error(err, "Failed to reconcile Container Toolkit Role")
		return err
	}
	h.log.Info("Reconciled Container Toolkit Role", "namespace", role.Namespace, "name", role.Name, "result", roleRes)
	return nil
}

func (h *containerToolkitPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile Container Toolkit RoleBinding")
		return err
	}
	h.log.Info("Reconciled Container Toolkit RoleBinding", "namespace", binding.Namespace, "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *containerToolkitPatcher) handleConfigMap(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewConfigMapBuilder(h.entrypointConfigMapName(), h.namespace)
	cm := builder.Build()

	script := strings.Join([]string{
		"#!/bin/sh",
		"",
		"until [ -f " + validationsMountPath + "/driver-ready ]",
		"do",
		"  echo \"waiting for the driver validations to be ready...\"",
		"  sleep 5",
		"done",
		"",
		"set -o allexport",
		". " + validationsMountPath + "/driver-ready",
		"",
		"command -v rbln-ctk >/dev/null 2>&1 && rbln-ctk info || true",
		"command -v rbln-ctk >/dev/null 2>&1 && rbln-ctk version || true",
		"command -v rbln-ctk-daemon >/dev/null 2>&1 && rbln-ctk-daemon --version || true",
		"echo \"driver-ready contents:\"",
		"cat " + validationsMountPath + "/driver-ready || true",
		"",
		"exec rbln-ctk-daemon \"$@\"",
	}, "\n") + "\n"

	cmRes, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm = builder.
			WithData(map[string]string{
				containerToolkitEntrypointKey: script,
			}).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Container Toolkit ConfigMap")
		return err
	}
	h.log.Info("Reconciled Container Toolkit ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", cmRes)
	return nil
}

func (h *containerToolkitPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	validatorSpec := owner.Spec.Validator
	driverArgs := containerToolkitValidatorArgs(validatorSpec.Args)
	baseEnv := mergeContainerToolkitEnv(
		validatorSpec.Env,
		validatorSpec.Driver.Env,
	)

	driverInit := k8sutil.NewContainerBuilder().
		WithName("driver-validation").
		WithImage(ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{validatorDefaultCommand}).
		WithArgs(driverArgs).
		WithEnvs(baseEnv).
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

	toolkitSpec := h.desiredSpec
	imagePullPolicy := toolkitSpec.ImagePullPolicy
	if imagePullPolicy == "" {
		imagePullPolicy = corev1.PullIfNotPresent
	}

	runtimeEnv := []corev1.EnvVar{}
	toolkitVolumeMounts := []corev1.VolumeMount{
		{
			Name:      validationsVolumeName,
			MountPath: validationsMountPath,
		},
		{
			Name:      validatorHostDriverVolumeName,
			MountPath: validatorHostDriverPath,
		},
		{
			Name:      validatorHostRootVolumeName,
			MountPath: "/host",
			ReadOnly:  true,
		},
		{
			Name:      containerToolkitCDIRootVolumeName,
			MountPath: containerToolkitCDIRootPath,
		},
		{
			Name:      containerToolkitRunVolumeName,
			MountPath: containerToolkitRunPath,
		},
		{
			Name:      containerToolkitHostBinVolumeName,
			MountPath: containerToolkitHostBinMountPath,
		},
		{
			Name:      h.entrypointConfigMapName(),
			MountPath: containerToolkitEntrypointPath,
			SubPath:   containerToolkitEntrypointKey,
			ReadOnly:  true,
		},
	}
	toolkitVolumes := []corev1.Volume{
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
			Name: containerToolkitCDIRootVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: containerToolkitCDIRootPath,
					Type: ptr(corev1.HostPathDirectoryOrCreate),
				},
			},
		},
		{
			Name: containerToolkitRunVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: containerToolkitRunPath,
					Type: ptr(corev1.HostPathDirectoryOrCreate),
				},
			},
		},
		{
			Name: containerToolkitHostBinVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: containerToolkitHostBinPath,
					Type: ptr(corev1.HostPathDirectoryOrCreate),
				},
			},
		},
		{
			Name: h.entrypointConfigMapName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: h.entrypointConfigMapName(),
					},
					Items: []corev1.KeyToPath{
						{
							Key:  containerToolkitEntrypointKey,
							Path: containerToolkitEntrypointKey,
						},
					},
					DefaultMode: ptr(int32(0o755)),
				},
			},
		},
	}

	switch h.containerRuntime {
	case consts.Containerd:
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.Containerd})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: containerdSockPath})
		toolkitVolumeMounts = append(toolkitVolumeMounts, corev1.VolumeMount{
			Name:      containerdSockVolumeName,
			MountPath: containerdSockPath,
		})
		toolkitVolumes = append(toolkitVolumes, corev1.Volume{
			Name: containerdSockVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: containerdSockPath,
					Type: ptr(corev1.HostPathSocket),
				},
			},
		})
	case consts.Docker:
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.Docker})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: dockerSockPath})
		toolkitVolumeMounts = append(toolkitVolumeMounts, corev1.VolumeMount{
			Name:      dockerSockVolumeName,
			MountPath: dockerSockPath,
		})
		toolkitVolumes = append(toolkitVolumes, corev1.Volume{
			Name: dockerSockVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: dockerSockPath,
					Type: ptr(corev1.HostPathSocket),
				},
			},
		})
	case consts.CRIO:
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.CRIO})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: crioSockPath})
		toolkitVolumeMounts = append(toolkitVolumeMounts, corev1.VolumeMount{
			Name:      crioSockVolumeName,
			MountPath: crioSockPath,
		})
		toolkitVolumes = append(toolkitVolumes, corev1.Volume{
			Name: crioSockVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: crioSockPath,
					Type: ptr(corev1.HostPathSocket),
				},
			},
		})
	}

	toolkitContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(ComposeImageReference(toolkitSpec.Registry, toolkitSpec.Image), toolkitSpec.Version, imagePullPolicy).
		WithCommands([]string{containerToolkitEntrypointPath}).
		WithEnvs(mergeContainerToolkitEnv(runtimeEnv, toolkitSpec.Env)).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithResources(toolkitSpec.Resources, "250m", "40Mi").
		WithVolumeMounts(toolkitVolumeMounts).
		Build()
	if len(toolkitSpec.Args) > 0 {
		toolkitContainer.Args = slices.Clone(toolkitSpec.Args)
	}

	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(k8sutil.NewPodSpecBuilder().
				WithServiceAccountName(h.name).
				WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.container-toolkit": "true"}).
				WithAffinity(h.desiredSpec.Affinity).
				WithTolerations(h.desiredSpec.Tolerations).
				WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
				WithPriorityClassName(h.desiredSpec.PriorityClassName).
				WithHostPID(true).
				WithVolumes(toolkitVolumes).
				WithInitContainers([]*corev1.Container{driverInit}).
				WithContainers([]*corev1.Container{toolkitContainer}).
				Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Container Toolkit DaemonSet")
		return err
	}

	h.log.Info("Reconciled Container Toolkit DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}

func containerToolkitValidatorArgs(baseArgs []string) []string {
	args := []string{validatorComponentDriver}
	if len(baseArgs) > 0 {
		args = append(args, baseArgs...)
	}
	return args
}

func mergeContainerToolkitEnv(base []corev1.EnvVar, additions ...[]corev1.EnvVar) []corev1.EnvVar {
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
