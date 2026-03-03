package patch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

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

func ComposeImageReference(registry, image string) string {
	registry = strings.TrimSuffix(strings.TrimSpace(registry), "/")
	image = strings.TrimPrefix(strings.TrimSpace(image), "/")
	return fmt.Sprintf("%s/%s", registry, image)
}

// syncSpec synchronizes a component spec with DaemonsetsSpec.
func syncSpec[T ComponentSpec](cpSpec *rblnv1beta1.RBLNClusterPolicySpec, componentSpec T) T {
	if !componentSpec.IsEnabled() {
		var zero T
		return zero
	}

	syncedSpec := componentSpec
	if cpSpec.Daemonsets != nil {
		ds := cpSpec.Daemonsets

		specValue := reflect.ValueOf(&syncedSpec).Elem()
		if labelsField := specValue.FieldByName("Labels"); labelsField.IsValid() && labelsField.CanSet() {
			labelsField.Set(reflect.ValueOf(k8sutil.MergeMaps(ds.Labels, labelsField.Interface().(map[string]string))))
		}
		if annotationsField := specValue.FieldByName("Annotations"); annotationsField.IsValid() && annotationsField.CanSet() {
			annotationsField.Set(reflect.ValueOf(k8sutil.MergeMaps(ds.Annotations, annotationsField.Interface().(map[string]string))))
		}
		if affinityField := specValue.FieldByName("Affinity"); affinityField.IsValid() && affinityField.CanSet() && affinityField.IsNil() && ds.Affinity != nil {
			affinityField.Set(reflect.ValueOf(ds.Affinity.DeepCopy()))
		}
		if tolerationsField := specValue.FieldByName("Tolerations"); tolerationsField.IsValid() && tolerationsField.CanSet() && tolerationsField.Len() == 0 && len(ds.Tolerations) > 0 {
			tolerations := make([]corev1.Toleration, len(ds.Tolerations))
			copy(tolerations, ds.Tolerations)
			tolerationsField.Set(reflect.ValueOf(tolerations))
		}
		if priorityField := specValue.FieldByName("PriorityClassName"); priorityField.IsValid() && priorityField.CanSet() && priorityField.String() == "" && ds.PriorityClassName != "" {
			priorityField.SetString(ds.PriorityClassName)
		}
	}

	return syncedSpec
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

func GetObjectHash(obj any) string {
	raw, err := json.Marshal(obj)
	if err != nil {
		// Fallback keeps function total-orderable even for non-JSON-serializable inputs.
		raw = fmt.Appendf(nil, "%#v", obj)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
