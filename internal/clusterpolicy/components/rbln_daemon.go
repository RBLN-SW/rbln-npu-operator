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

const (
	rblnDaemonDefaultHostPort = 50051
	rblnDaemonPortName        = "rbln-daemon"
	rblnDaemonCommand         = "/opt/rebellions/bin/rbln_daemon"

	rblnDaemonSysVolumeName    = "host-sys"
	rblnDaemonSysPath          = "/sys"
	rblnDaemonDebugVolumeName  = "host-debug"
	rblnDaemonDebugPath        = "/sys/kernel/debug"
	rblnDaemonLogVolumeName    = "host-log-rebellions"
	rblnDaemonLogPath          = "/var/log/rebellions"
	rblnDaemonVarRunVolumeName = "host-var-run"
	rblnDaemonVarRunPath       = "/var/run"
)

type rblnDaemonPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNDaemonSpec
	podDefaults *rblnv1beta1.PodDefaultsSpec
}

func NewRBLNDaemonPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.RBLNDaemon)
	return &rblnDaemonPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNDaemonName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigContainer,
		},
		desiredSpec: &synced,
		podDefaults: cpSpec.PodDefaults,
	}
}

func (h *rblnDaemonPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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

func (h *rblnDaemonPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "RBLN Daemon")
	if err := h.deleteIfExists(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
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

func (h *rblnDaemonPatcher) handleService(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}
	labelsMap := selectorLabels(consts.RBLNDaemonName)

	res, err := controllerutil.CreateOrPatch(ctx, h.client, svc, func() error {
		svc.Labels = labelsMap
		svc.Spec.Selector = labelsMap
		svc.Spec.Ports = []corev1.ServicePort{{Name: rblnDaemonPortName, Port: rblnDaemonDefaultHostPort, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(rblnDaemonDefaultHostPort)}}
		svc.Spec.InternalTrafficPolicy = ptr(corev1.ServiceInternalTrafficPolicyLocal)
		return ctrl.SetControllerReference(owner, svc, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon Service")
		return err
	}
	h.log.Info("Reconciled RBLNDaemon Service", "namespace", svc.Namespace, "name", svc.Name, "result", res)
	return nil
}

func (h *rblnDaemonPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildToolkitValidationInitContainer(owner.Spec.Validator)

	daemonContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithCommands([]string{rblnDaemonCommand}).
		WithEnvs(h.desiredSpec.Env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithPorts([]corev1.ContainerPort{
			{
				Name:          rblnDaemonPortName,
				ContainerPort: rblnDaemonDefaultHostPort,
				Protocol:      corev1.ProtocolTCP,
			},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: rblnDaemonVarRunVolumeName, MountPath: rblnDaemonVarRunPath},
			// /sys must be read-write: smd's EnableVfs writes
			// /sys/bus/pci/devices/<bdf>/sriov_numvfs to partition PFs into
			// VFs, and a ReadOnly hostPath mount fails that with EROFS even
			// in a privileged container. Scoping rw to /sys/bus/pci does not
			// work either — its entries are symlinks back into /sys/devices.
			{Name: rblnDaemonSysVolumeName, MountPath: rblnDaemonSysPath},
			{Name: rblnDaemonDebugVolumeName, MountPath: rblnDaemonDebugPath},
			{Name: rblnDaemonLogVolumeName, MountPath: rblnDaemonLogPath},
		}).
		Build()

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithHostNetwork(true).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.rbln-daemon": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(podDefaultsPriorityClassName(h.podDefaults)).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume(rblnDaemonVarRunVolumeName, rblnDaemonVarRunPath, corev1.HostPathDirectory),
			hostPathVolume(rblnDaemonSysVolumeName, rblnDaemonSysPath, corev1.HostPathDirectory),
			hostPathVolume(rblnDaemonDebugVolumeName, rblnDaemonDebugPath, corev1.HostPathDirectory),
			hostPathVolume(rblnDaemonLogVolumeName, rblnDaemonLogPath, corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{daemonContainer}).
		WithTerminationGracePeriodSeconds(0).
		Build()
}

func (h *rblnDaemonPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	sel := selectorLabels(consts.RBLNDaemonName)

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
		h.log.Error(err, "Failed to reconcile RBLNDaemon DaemonSet")
		return err
	}
	h.log.Info("Reconciled RBLNDaemon DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
