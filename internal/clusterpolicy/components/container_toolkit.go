package components

import (
	"context"
	"slices"
	"strings"

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
	basePatcher
	desiredSpec      *rblnv1beta1.RBLNContainerToolkitSpec
	containerRuntime string
}

func NewContainerToolkitPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string, containerRuntime string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.ContainerToolkit)
	return &containerToolkitPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             cpSpec.BaseName + "-" + consts.RBLNContainerToolkitName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec:      &synced,
		containerRuntime: containerRuntime,
	}
}

func (h *containerToolkitPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	// ContainerToolkit always needs RBAC (apps/daemonsets list), not just on OpenShift.
	if err := h.reconcileContainerToolkitRole(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *containerToolkitPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Container Toolkit is disabled. Remove all Container Toolkit resources")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.entrypointConfigMapName(), Namespace: h.namespace},
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

func (h *containerToolkitPatcher) entrypointConfigMapName() string {
	return h.name + "-entrypoint"
}

// reconcileContainerToolkitRole creates a Role that always includes apps/daemonsets access,
// and additionally grants OpenShift SCC privileges when running on OpenShift.
func (h *containerToolkitPatcher) reconcileContainerToolkitRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
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
		role.Rules = rules
		return ctrl.SetControllerReference(owner, role, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Container Toolkit Role", "name", h.name)
		return err
	}
	h.log.Info("Reconciled Container Toolkit Role", "name", role.Name, "namespace", role.Namespace, "result", res)
	return nil
}

func (h *containerToolkitPatcher) handleConfigMap(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.entrypointConfigMapName(), Namespace: h.namespace},
	}

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

	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = map[string]string{containerToolkitEntrypointKey: script}
		return ctrl.SetControllerReference(owner, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Container Toolkit ConfigMap")
		return err
	}
	h.log.Info("Reconciled Container Toolkit ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", res)
	return nil
}

func (h *containerToolkitPatcher) buildRuntimeMountsAndVolumes() ([]corev1.VolumeMount, []corev1.Volume) {
	switch h.containerRuntime {
	case consts.Containerd:
		return []corev1.VolumeMount{
				{Name: containerdSockVolumeName, MountPath: containerdSockPath},
			}, []corev1.Volume{
				hostPathVolume(containerdSockVolumeName, containerdSockPath, corev1.HostPathSocket),
			}
	case consts.Docker:
		return []corev1.VolumeMount{
				{Name: dockerSockVolumeName, MountPath: dockerSockPath},
			}, []corev1.Volume{
				hostPathVolume(dockerSockVolumeName, dockerSockPath, corev1.HostPathSocket),
			}
	case consts.CRIO:
		return []corev1.VolumeMount{
				{Name: crioSockVolumeName, MountPath: crioSockPath},
			}, []corev1.Volume{
				hostPathVolume(crioSockVolumeName, crioSockPath, corev1.HostPathSocket),
			}
	default:
		return nil, nil
	}
}

func (h *containerToolkitPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	validatorSpec := owner.Spec.Validator
	driverArgs := containerToolkitValidatorArgs(validatorSpec.Args)
	baseEnv := mergeEnvVars(validatorSpec.Env, validatorSpec.Driver.Env)

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

	runtimeMounts, runtimeVolumes := h.buildRuntimeMountsAndVolumes()

	runtimeEnv := []corev1.EnvVar{}
	switch h.containerRuntime {
	case consts.Containerd:
		runtimeEnv = append(runtimeEnv,
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.Containerd},
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: containerdSockPath},
		)
	case consts.Docker:
		runtimeEnv = append(runtimeEnv,
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.Docker},
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: dockerSockPath},
		)
	case consts.CRIO:
		runtimeEnv = append(runtimeEnv,
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_RUNTIME", Value: consts.CRIO},
			corev1.EnvVar{Name: "RBLN_CTK_DAEMON_SOCKET", Value: crioSockPath},
		)
	}

	toolkitVolumeMounts := []corev1.VolumeMount{
		{Name: validationsVolumeName, MountPath: validationsMountPath},
		{Name: validatorHostDriverVolumeName, MountPath: validatorHostDriverPath},
		{Name: validatorHostRootVolumeName, MountPath: "/host", ReadOnly: true},
		{Name: containerToolkitCDIRootVolumeName, MountPath: containerToolkitCDIRootPath},
		{Name: containerToolkitRunVolumeName, MountPath: containerToolkitRunPath},
		{Name: containerToolkitHostBinVolumeName, MountPath: containerToolkitHostBinMountPath},
		{
			Name:      h.entrypointConfigMapName(),
			MountPath: containerToolkitEntrypointPath,
			SubPath:   containerToolkitEntrypointKey,
			ReadOnly:  true,
		},
	}
	toolkitVolumeMounts = append(toolkitVolumeMounts, runtimeMounts...)

	toolkitContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(ComposeImageReference(toolkitSpec.Registry, toolkitSpec.Image), toolkitSpec.Version, imagePullPolicy).
		WithCommands([]string{containerToolkitEntrypointPath}).
		WithEnvs(mergeEnvVars(runtimeEnv, toolkitSpec.Env)).
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

	toolkitVolumes := []corev1.Volume{
		hostPathVolume(validationsVolumeName, validationsMountPath, corev1.HostPathDirectoryOrCreate),
		hostPathVolume(validatorHostDriverVolumeName, validatorHostDriverPath, corev1.HostPathDirectoryOrCreate),
		hostPathVolume(validatorHostRootVolumeName, validatorHostRootPath, corev1.HostPathDirectory),
		hostPathVolume(containerToolkitCDIRootVolumeName, containerToolkitCDIRootPath, corev1.HostPathDirectoryOrCreate),
		hostPathVolume(containerToolkitRunVolumeName, containerToolkitRunPath, corev1.HostPathDirectoryOrCreate),
		hostPathVolume(containerToolkitHostBinVolumeName, containerToolkitHostBinPath, corev1.HostPathDirectoryOrCreate),
		{
			Name: h.entrypointConfigMapName(),
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: h.entrypointConfigMapName()},
				Items: []corev1.KeyToPath{
					{Key: containerToolkitEntrypointKey, Path: containerToolkitEntrypointKey},
				},
				DefaultMode: ptr(int32(0o755)),
			}},
		},
	}
	toolkitVolumes = append(toolkitVolumes, runtimeVolumes...)

	return k8sutil.NewPodSpecBuilder().
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
		Build()
}

func (h *containerToolkitPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile Container Toolkit DaemonSet")
		return err
	}
	h.log.Info("Reconciled Container Toolkit DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}

func containerToolkitValidatorArgs(baseArgs []string) []string {
	args := []string{validatorComponentDriver}
	if len(baseArgs) > 0 {
		args = append(args, baseArgs...)
	}
	return args
}
