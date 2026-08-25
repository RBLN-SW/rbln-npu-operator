package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

type metricsExporterPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNMetricsExporterSpec
}

func NewMetricsExporterPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.MetricsExporter)
	return &metricsExporterPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNMetricExporterName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigContainer,
		},
		desiredSpec: &synced,
	}
}

func (h *metricsExporterPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}
	return h.handleService(ctx, owner)
}

func (h *metricsExporterPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.VDebug).Info("Cleaning up disabled component", "component", "Metrics Exporter")
	if err := h.deleteIfExists(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-service", Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *metricsExporterPatcher) handleService(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: h.name + "-service", Namespace: h.namespace},
	}
	labelsMap := selectorLabels(consts.RBLNMetricExporterName)

	res, err := controllerutil.CreateOrPatch(ctx, h.client, svc, func() error {
		svc.Labels = labelsMap
		svc.Annotations = map[string]string{
			"prometheus.io/scrape": "true",
			"prometheus.io/path":   "/metrics",
			"prometheus.io/port":   "9090",
		}
		svc.Spec.Selector = labelsMap
		svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 9090, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(9090)}}
		return ctrl.SetControllerReference(cp, svc, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile MetricsExporter Service")
		return err
	}
	h.log.Info("Reconciled MetricsExporter Service", "namespace", svc.Namespace, "name", svc.Name, "result", res)
	return nil
}

func (h *metricsExporterPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildToolkitValidationInitContainer(owner.Spec.Validator)

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.metrics-exporter": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("pod-resources", "/var/lib/kubelet/pod-resources", corev1.HostPathDirectory),
			hostPathVolume("sysfs", "/sys", corev1.HostPathDirectory),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{
			k8sutil.NewContainerBuilder().
				WithName(h.name).
				WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
				WithVolumeMounts([]corev1.VolumeMount{
					{Name: "pod-resources", MountPath: "/var/lib/kubelet/pod-resources", ReadOnly: true},
					{Name: "sysfs", MountPath: "/sys", ReadOnly: true},
				}).
				WithEnvs([]corev1.EnvVar{
					{
						Name: "NODE_IP",
						ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
							APIVersion: "v1", FieldPath: "status.hostIP",
						}},
					},
					{
						Name: "NODE_NAME",
						ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
							APIVersion: "v1", FieldPath: "spec.nodeName",
						}},
					},
					{Name: "RBLN_METRICS_EXPORTER_RBLN_DAEMON_URL", Value: "http://$(NODE_IP):50051"},
					{Name: "PROMETHEUS_METRIC_NAMES", Value: "true"},
				}).
				WithResources(h.desiredSpec.Resources, "250m", "40Mi").
				WithSecurityContext(&corev1.SecurityContext{
					Privileged:             ptr(true),
					RunAsUser:              ptr(int64(0)),
					RunAsGroup:             ptr(int64(0)),
					ReadOnlyRootFilesystem: ptr(false),
				}).
				Build(),
		}).
		WithTerminationGracePeriodSeconds(0).
		Build()
}

func (h *metricsExporterPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	sel := selectorLabels(consts.RBLNMetricExporterName)

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(sel).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile MetricsExporter DaemonSet")
		return err
	}
	h.log.Info("Reconciled MetricsExporter DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
