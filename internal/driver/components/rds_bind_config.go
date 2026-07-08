package components

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func (h *driverManagerPatcher) rdsBindConfigMapName() string {
	return fmt.Sprintf("%s-%s", h.name, rdsBindConfigMapSuffix)
}

func buildRDSBindConfigData(selection map[string][]string) map[string]string {
	data := make(map[string]string, len(selection))
	for node, bdfs := range selection {
		// A node with no BDFs falls back to the bind script's auto opt-out.
		if len(bdfs) == 0 {
			continue
		}
		data[node] = fmt.Sprintf("TARGET_BDFS=%q\n", strings.Join(bdfs, " "))
	}
	return data
}

// The ConfigMap is rendered even when empty so the -rds pod's optional volume
// reference always resolves; when RDS is disabled it is removed instead.
func (h *driverManagerPatcher) handleRDSBindConfigMap(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.rdsBindConfigMapName(),
			Namespace: h.namespace,
		},
	}

	if !h.rdsBindingEnabled {
		return h.deleteIfExists(ctx, cm)
	}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm.Data = buildRDSBindConfigData(h.rdsDeviceSelection)
		return controllerutil.SetOwnerReference(owner, cm, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RDS bind-config ConfigMap")
		return err
	}
	h.log.Info("Reconciled RDS bind-config ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", res)
	return nil
}

// A node with no ConfigMap entry gets an empty conf file, so the bind script
// falls back to its default auto opt-out.
func (h *driverManagerPatcher) buildRDSBindConfigSelectInitContainer() *corev1.Container {
	script := `set -eu
src="` + rdsBindConfigMountDir + `/${NODE_NAME}"
dst="` + rdsBindConfWorkDir + `/` + rdsBindConfFileName + `"
if [ -f "$src" ]; then
  cp "$src" "$dst"
else
  : > "$dst"
fi
`
	return k8sutil.NewContainerBuilder().
		WithName(rdsBindConfigSelectInitContainer).
		WithImage(k8sutil.ComposeImageReference(
			h.desiredSpec.Manager.Registry, h.desiredSpec.Manager.Image),
			h.desiredSpec.Manager.Version,
			h.desiredSpec.Manager.ImagePullPolicy,
		).
		WithCommands([]string{"/bin/sh", "-c", script}).
		WithEnvs([]corev1.EnvVar{
			{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "spec.nodeName"},
			}},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: rdsBindConfigVolumeName, MountPath: rdsBindConfigMountDir, ReadOnly: true},
			{Name: rdsBindConfVolumeName, MountPath: rdsBindConfWorkDir},
		}).
		Build()
}

func (h *driverManagerPatcher) rdsBindVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: rdsBindConfigVolumeName,
			// Optional so the pod still starts (auto opt-out) if the ConfigMap
			// is briefly absent.
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: h.rdsBindConfigMapName()},
				Optional:             ptr(true),
			}},
		},
		{
			Name:         rdsBindConfVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
}

func rdsBindConfVolumeMount() corev1.VolumeMount {
	// subPath mounts only the conf file, leaving the rest of /etc/rebellions intact.
	return corev1.VolumeMount{
		Name:      rdsBindConfVolumeName,
		MountPath: rdsBindConfContainerPath,
		SubPath:   rdsBindConfFileName,
		ReadOnly:  true,
	}
}
