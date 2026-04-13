package components

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

type mountPathToVolumeSource map[string]corev1.VolumeSource

var subscriptionPathMap = map[string]mountPathToVolumeSource{
	"rhel": {
		"/etc/pki/entitlement": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/pki/entitlement", Type: ptr(corev1.HostPathDirectory),
		}},
		"/etc/rhsm": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/rhsm", Type: ptr(corev1.HostPathDirectory),
		}},
		"/etc/yum.repos.d": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/yum.repos.d", Type: ptr(corev1.HostPathDirectory),
		}},
	},
	"rhcos": {
		"/var/run/secrets/etc-pki-entitlement": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/pki/entitlement", Type: ptr(corev1.HostPathDirectory),
		}},
		"/var/run/secrets/rhsm": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/rhsm", Type: ptr(corev1.HostPathDirectory),
		}},
		"/etc/yum.repos.d": {HostPath: &corev1.HostPathVolumeSource{
			Path: "/etc/yum.repos.d", Type: ptr(corev1.HostPathDirectory),
		}},
	},
}

func getSubscriptionPathsToVolumeSources(os string) (mountPathToVolumeSource, error) {
	if m, ok := subscriptionPathMap[os]; ok {
		return m, nil
	}
	return nil, fmt.Errorf("distribution %s not supported", os)
}

// ─── ConfigMap ──────────────────────────────────────────────────────────────

func (h *driverManagerPatcher) handleConfigMap(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	script := `#!/bin/sh
set -eu

VALIDATIONS_DIR="` + consts.ValidationsMountPath + `"
READY_FILE="${VALIDATIONS_DIR}/.driver-ctr-ready"

mkdir -p "${VALIDATIONS_DIR}"

if [ ! -f /sys/module/rebellions/refcnt ]; then
  echo "Rebellions kernel module not loaded"
  exit 1
fi

if ! command -v rbln-smi >/dev/null 2>&1; then
  echo "rbln-smi not found"
  exit 1
fi

if ! rbln-smi; then
  echo "rbln-smi failed"
  exit 1
fi

TMP_FILE="${READY_FILE}.tmp"
: > "$TMP_FILE"
mv "$TMP_FILE" "$READY_FILE"
`

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.startupProbeConfigMapName(),
			Namespace: h.namespace,
		},
	}
	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = map[string]string{startupProbeScriptName: script}
		return controllerutil.SetOwnerReference(owner, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile startup probe ConfigMap")
		return err
	}
	h.log.Info("Reconciled startup probe ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", res)
	return nil
}

// ─── Pod spec builders ──────────────────────────────────────────────────────

func (h *driverManagerPatcher) buildDriverPodSpec(pool nodePool) (*corev1.PodSpec, error) {
	additionalVolumeMounts, additionalVolumes, err := h.buildSubscriptionMountsAndVolumes(pool)
	if err != nil {
		return nil, err
	}

	initContainer := h.buildDriverManagerInitContainer()

	driverContainer, err := h.buildDriverContainer(pool, additionalVolumeMounts)
	if err != nil {
		return nil, err
	}

	volumes := []corev1.Volume{
		{
			Name: hostDriverVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: hostDriverPath, Type: ptr(corev1.HostPathDirectoryOrCreate),
			}},
		},
		{
			Name: consts.ValidationsVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: consts.ValidationsMountPath, Type: ptr(corev1.HostPathDirectoryOrCreate),
			}},
		},
		{
			Name: hostRootVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: hostRootPath, Type: ptr(corev1.HostPathDirectory),
			}},
		},
		{
			Name: hostDevVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: hostDevPath, Type: ptr(corev1.HostPathDirectory),
			}},
		},
		{
			Name: h.startupProbeConfigMapName(),
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: h.startupProbeConfigMapName()},
				Items:                []corev1.KeyToPath{{Key: startupProbeScriptName, Path: startupProbeScriptName, Mode: ptr(int32(0o755))}},
			}},
		},
	}
	volumes = append(volumes, additionalVolumes...)

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(pool.nodeSelector).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(h.desiredSpec.PriorityClassName).
		WithVolumes(volumes).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{driverContainer}).
		Build(), nil
}

func (h *driverManagerPatcher) buildDriverManagerInitContainer() *corev1.Container {
	return k8sutil.NewContainerBuilder().
		WithName(driverManagerInitContainer).
		WithImage(k8sutil.ComposeImageReference(
			h.desiredSpec.Manager.Registry, h.desiredSpec.Manager.Image),
			h.desiredSpec.Manager.Version,
			h.desiredSpec.Manager.ImagePullPolicy,
		).
		WithCommands([]string{driverManagerCommand}).
		WithArgs([]string{driverManagerSyncDriverLabel}).
		WithEnvs([]corev1.EnvVar{
			{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			}},
			{Name: "ENABLE_NPU_POD_EVICTION", Value: "true"},
			{Name: "ENABLE_AUTO_DRAIN", Value: "false"},
			{Name: "DRAIN_USE_FORCE", Value: "false"},
			{Name: "DRAIN_POD_SELECTOR_LABEL", Value: ""},
			{Name: "DRAIN_TIMEOUT_SECONDS", Value: "0s"},
			{Name: "DRAIN_DELETE_EMPTYDIR_DATA", Value: "false"},
			{Name: "OPERATOR_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			}},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged:     ptr(true),
			SELinuxOptions: &corev1.SELinuxOptions{Level: "s0"},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             hostRootVolumeName,
				MountPath:        "/host",
				ReadOnly:         true,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
}

func (h *driverManagerPatcher) buildDriverContainer(
	pool nodePool,
	additionalVolumeMounts []corev1.VolumeMount,
) (*corev1.Container, error) {
	imagePath, err := h.desiredSpec.GetPrecompiledImagePath(pool.getOS(), pool.kernel)
	if err != nil {
		return nil, err
	}

	pullPolicy := h.resolveImagePullPolicy()

	volumeMounts := []corev1.VolumeMount{
		{
			Name:             hostDriverVolumeName,
			MountPath:        "/host/run/rbln/driver",
			MountPropagation: ptr(corev1.MountPropagationBidirectional),
		},
		{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
		{Name: hostDevVolumeName, MountPath: hostDevPath},
		{
			Name:      h.startupProbeConfigMapName(),
			MountPath: startupProbeScriptPath,
			SubPath:   startupProbeScriptName,
			ReadOnly:  true,
		},
	}
	volumeMounts = append(volumeMounts, additionalVolumeMounts...)

	container := k8sutil.NewContainerBuilder().
		WithName(driverManagerContainer).
		WithCommands([]string{driverInstallerCommand}).
		WithArgs([]string{driverInstallerInitArg}).
		WithEnvs(h.desiredSpec.Env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithLifeCycle(&corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"/bin/sh", "-c", "rm -f " + consts.ValidationsMountPath + "/.driver-ctr-ready"},
				},
			},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged:     ptr(true),
			RunAsUser:      ptr(int64(0)),
			SELinuxOptions: &corev1.SELinuxOptions{Level: "s0"},
		}).
		WithVolumeMounts(volumeMounts).
		Build()

	container.Image = imagePath
	container.ImagePullPolicy = pullPolicy
	container.StartupProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", startupProbeScriptPath},
			},
		},
		TimeoutSeconds:   driverManagerStartupProbeTimeoutSeconds,
		PeriodSeconds:    driverManagerStartupProbePeriodSeconds,
		FailureThreshold: driverManagerStartupProbeFailureThreshold,
	}

	return container, nil
}

func (h *driverManagerPatcher) resolveImagePullPolicy() corev1.PullPolicy {
	policy := h.desiredSpec.ImagePullPolicy
	if policy == "" {
		policy = corev1.PullIfNotPresent
	}
	if h.desiredSpec.Version == "latest" {
		policy = corev1.PullAlways
	}
	return policy
}

func (h *driverManagerPatcher) subscriptionOS(pool nodePool) string {
	if h.openshiftVersion != "" {
		return "rhcos"
	}
	if pool.osRelease == "rhel" {
		return "rhel"
	}
	return ""
}

func (h *driverManagerPatcher) buildSubscriptionMountsAndVolumes(
	pool nodePool,
) ([]corev1.VolumeMount, []corev1.Volume, error) {
	osType := h.subscriptionOS(pool)
	if osType == "" {
		return nil, nil, nil
	}

	h.log.Info("Mounting subscription entitlements into driver container", "os", osType, "nodePool", pool.name)
	pathToVolumeSource, err := getSubscriptionPathsToVolumeSources(osType)
	if err != nil {
		return nil, nil, err
	}

	mountPaths := make([]string, 0, len(pathToVolumeSource))
	for mountPath := range pathToVolumeSource {
		mountPaths = append(mountPaths, mountPath)
	}
	sort.Strings(mountPaths)

	additionalVolumeMounts := make([]corev1.VolumeMount, 0, len(pathToVolumeSource))
	additionalVolumes := make([]corev1.Volume, 0, len(pathToVolumeSource))
	for i, mountPath := range mountPaths {
		volName := fmt.Sprintf("subscription-config-%d", i)
		additionalVolumeMounts = append(additionalVolumeMounts, corev1.VolumeMount{
			Name: volName, MountPath: mountPath, ReadOnly: true,
		})
		additionalVolumes = append(additionalVolumes, corev1.Volume{
			Name: volName, VolumeSource: pathToVolumeSource[mountPath],
		})
	}

	return additionalVolumeMounts, additionalVolumes, nil
}
