package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

type npuFeatureDiscoveryPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNNPUFeatureDiscoverySpec
}

func NewNPUFeatureDiscoveryPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.NPUFeatureDiscovery)
	return &npuFeatureDiscoveryPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             cpSpec.BaseName + "-" + consts.RBLNFeatureDiscoveryName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
		},
		desiredSpec: &synced,
	}
}

func (h *npuFeatureDiscoveryPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *npuFeatureDiscoveryPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "NPU Feature Discovery")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *npuFeatureDiscoveryPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildToolkitValidationInitContainer(owner.Spec.Validator)

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.npu-feature-discovery": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("features-dir", "/etc/kubernetes/node-feature-discovery/features.d", corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithTerminationGracePeriodSeconds(0).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithEnvs([]corev1.EnvVar{
					{
						Name: "NODE_IP",
						ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
							APIVersion: "v1", FieldPath: "status.hostIP",
						}},
					},
				}).
				WithResources(h.desiredSpec.Resources, "250m", "40Mi").
				WithArgs([]string{"--rbln-daemon-url", "http://$(NODE_IP):50051"}).
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: "features-dir", MountPath: "/etc/kubernetes/node-feature-discovery/features.d"},
				}).
				WithSecurityContext(&corev1.SecurityContext{
					Privileged: ptr(true),
					RunAsUser:  ptr(int64(0)),
				}).
				Build(),
		}).
		Build()
}

func (h *npuFeatureDiscoveryPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile NPUFeatureDiscovery DaemonSet")
		return err
	}
	h.log.Info("Reconciled NPUFeatureDiscovery DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
