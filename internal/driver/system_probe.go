package driver

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RBLN-SW/rbln-npu-operator/internal/driver/components"
)

// The probe follows the running operator image, independently of an old or
// explicitly selected driver-manager/validator image. Local controller runs
// may supply a built operator image through RBLN_SYSTEM_PROBE_IMAGE.
func resolveSystemProbeImage(ctx context.Context, reader client.Reader, namespace string) (components.SystemProbeImage, error) {
	if ref := os.Getenv("RBLN_SYSTEM_PROBE_IMAGE"); ref != "" {
		return components.SystemProbeImage{Reference: ref, PullPolicy: corev1.PullIfNotPresent}, nil
	}
	name := os.Getenv("POD_NAME")
	if name == "" {
		var err error
		name, err = os.Hostname()
		if err != nil {
			return components.SystemProbeImage{}, err
		}
	}
	var pod corev1.Pod
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &pod); err != nil {
		return components.SystemProbeImage{}, fmt.Errorf("resolve operator image for system probe: %w", err)
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == "manager" {
			return components.SystemProbeImage{Reference: c.Image, PullPolicy: c.ImagePullPolicy, PullSecrets: append([]corev1.LocalObjectReference(nil), pod.Spec.ImagePullSecrets...)}, nil
		}
	}
	return components.SystemProbeImage{}, fmt.Errorf("operator Pod %s/%s has no manager container", namespace, name)
}
