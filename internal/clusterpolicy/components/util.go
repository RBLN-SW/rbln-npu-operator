package components

import (
	"fmt"
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

// ComponentSpec defines the common interface for component specs.
type ComponentSpec interface {
	IsEnabled() bool
}

type deviceSelector struct {
	Vendors []string `json:"vendors"`
	Devices []string `json:"devices"`
	Drivers []string `json:"drivers"`
}

type configResource struct {
	ResourceName   string         `json:"resourceName"`
	ResourcePrefix string         `json:"resourcePrefix"`
	DeviceType     string         `json:"deviceType"`
	Selectors      deviceSelector `json:"selectors"`
}

type configResourceList struct {
	ResourceList []configResource `json:"resourceList"`
}

func ptr[T any](v T) *T {
	return &v
}

// syncSpec synchronizes a component spec with PodDefaultsSpec.
// Global values are inherited only when the component does not define its own.
func syncSpec[T ComponentSpec](cpSpec *rblnv1beta1.RBLNClusterPolicySpec, componentSpec T) T {
	if !componentSpec.IsEnabled() {
		var zero T
		return zero
	}

	syncedSpec := componentSpec
	if cpSpec.PodDefaults != nil {
		ds := cpSpec.PodDefaults

		specValue := reflect.ValueOf(&syncedSpec).Elem()
		if labelsField := specValue.FieldByName("Labels"); labelsField.IsValid() && labelsField.CanSet() {
			labelsField.Set(reflect.ValueOf(k8sutil.MergeMaps(ds.Labels, labelsField.Interface().(map[string]string))))
		}
		if annotationsField := specValue.FieldByName("Annotations"); annotationsField.IsValid() && annotationsField.CanSet() {
			annotationsField.Set(reflect.ValueOf(k8sutil.MergeMaps(ds.Annotations, annotationsField.Interface().(map[string]string))))
		}
		if tolerationsField := specValue.FieldByName("Tolerations"); tolerationsField.IsValid() && tolerationsField.CanSet() && tolerationsField.Len() == 0 && len(ds.Tolerations) > 0 {
			tolerations := make([]corev1.Toleration, len(ds.Tolerations))
			copy(tolerations, ds.Tolerations)
			tolerationsField.Set(reflect.ValueOf(tolerations))
		}
	}

	return syncedSpec
}

// podDefaultsPriorityClassName returns PriorityClassName from PodDefaults, or empty if nil.
func podDefaultsPriorityClassName(pd *rblnv1beta1.PodDefaultsSpec) string {
	if pd != nil {
		return pd.PriorityClassName
	}
	return ""
}

// hostPathVolume is a concise constructor for HostPath-backed volumes.
func hostPathVolume(name, path string, t corev1.HostPathType) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: path,
				Type: ptr(t),
			},
		},
	}
}

// mergeEnvVars merges one or more lists of env vars into base,
// with later entries overriding earlier ones when names collide.
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

// envVarValue returns the value of the first env var with the given name,
// or empty string and false if not found.
func envVarValue(envs []corev1.EnvVar, name string) (string, bool) {
	for _, e := range envs {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func collectDevices(productCardNames []string) ([]string, error) {
	devices := make([]string, 0)
	for _, productCardName := range productCardNames {
		deviceList, ok := consts.DeviceMapping[productCardName]
		if !ok {
			return nil, fmt.Errorf("unknown product card name: %s", productCardName)
		}
		devices = append(devices, deviceList...)
	}
	return devices, nil
}
