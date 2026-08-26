package components

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

// SmdPatcher reconciles the per-CR rbln-smd DaemonSet. Unlike the driver it is
// not partitioned into OS/kernel pools: one DaemonSet per RBLNDriver instance,
// selected onto that instance's owner-labeled nodes, with its image tag pinned
// to the driver's spec.version.
type SmdPatcher struct {
	basePatcher
	desiredSpec *rebellionsaiv1alpha1.RBLNDriverSpec
}

func NewSmdPatcher(
	client client.Client,
	apiReader client.Reader,
	log logr.Logger,
	namespace string,
	driver *rebellionsaiv1alpha1.RBLNDriver,
	scheme *runtime.Scheme,
	openshiftVersion string,
) (*SmdPatcher, error) {
	if driver == nil {
		return nil, fmt.Errorf("driver is nil")
	}
	return &SmdPatcher{
		basePatcher: basePatcher{
			client:           client,
			apiReader:        apiReader,
			log:              log,
			scheme:           scheme,
			name:             smdName,
			instanceName:     driver.Name,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
		},
		desiredSpec: &driver.Spec,
	}, nil
}

// Patch reconciles the shared rbln-smd RBAC and this instance's DaemonSet.
// driverDeployed gates the DaemonSet on "this CR rendered at least one driver
// DaemonSet": smd runs only where the driver runs, and without a driver there
// is no version to align to.
func (h *SmdPatcher) Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver, driverDeployed bool) error {
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}

	if !driverDeployed {
		return h.deleteIfExists(ctx, &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: h.daemonSetName(), Namespace: h.namespace},
		})
	}

	version := strings.TrimSpace(h.desiredSpec.Version)
	if version == "" {
		// Driver DaemonSets can outlive a spec whose version was since
		// emptied; rendering here would fall back to :latest and break the
		// driver/smd version alignment this component exists for. Keep
		// whatever is running.
		h.log.V(consts.VDebug).Info("Driver version is empty; keeping the existing rbln-smd DaemonSet as-is",
			"driver", h.instanceName)
		return nil
	}

	// Migration from the RBLNClusterPolicy-owned rbln-daemon: that DaemonSet
	// binds the same host port, so delete it before creating ours — the
	// clusterpolicy controller's legacy cleanup also deletes it, but only this
	// ordering guarantees the two never overlap on a node.
	// TODO(remove after two releases): drop together with legacyRBLNDaemonName.
	if err := h.deleteIfExists(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: h.namespace},
	}); err != nil {
		return err
	}

	return h.handleDaemonSet(ctx, owner, version)
}

// Status reports the smd DaemonSet's readiness; nil when it does not exist.
// Uses apiReader so a Delete issued earlier in this same reconcile is
// observed rather than served stale from the informer cache.
func (h *SmdPatcher) Status(ctx context.Context) (*rebellionsaiv1alpha1.RBLNDriverSmdStatus, error) {
	ds := &appsv1.DaemonSet{}
	if err := h.apiReader.Get(ctx, client.ObjectKey{Name: h.daemonSetName(), Namespace: h.namespace}, ds); err != nil {
		if kapierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get rbln-smd DaemonSet: %w", err)
	}

	desired := ds.Status.DesiredNumberScheduled
	ready := ds.Status.NumberReady
	state := rebellionsaiv1alpha1.DriverPoolStateReady
	if desired == 0 || ready != desired || ds.Status.NumberUnavailable > 0 {
		state = rebellionsaiv1alpha1.DriverPoolStateProgressing
	}
	return &rebellionsaiv1alpha1.RBLNDriverSmdStatus{Desired: desired, Ready: ready, State: state}, nil
}

func (h *SmdPatcher) daemonSetName() string {
	return h.instanceName + "-" + smdDaemonSetSuffix
}

// smdLabels deliberately sets app.kubernetes.io/component to rbln-smd, not
// rbln-driver: the upgrade controller discovers driver DaemonSets by that
// exact component value and must never sweep smd into its state machine.
func (h *SmdPatcher) smdLabels() map[string]string {
	return map[string]string{
		driverManagerAppNameLabelKey:  consts.OperatorName,
		driverManagerAppLabelKey:      h.name,
		driverManagerInstanceLabelKey: h.instanceName,
	}
}

// smdNodeSelector pins smd pods to this instance's driver nodes. The owner
// label alone scopes the instance; npu.deploy.rbln-smd is additionally
// required because it is the eviction handle k8s-driver-manager flips to
// paused-for-driver-upgrade on every driver pod start, which (with OnDelete)
// is what rolls this pod onto the current template in step with the driver.
// The key name is that binary's hardcoded contract — do not rename it here
// alone, and note that a driver-manager predating the rename never flips it.
func (h *SmdPatcher) smdNodeSelector() map[string]string {
	return map[string]string{
		driverManagerDeployLabelKey:    "true",
		consts.RBLNDriverOwnerLabelKey: h.instanceName,
		consts.RBLNDeploySmdLabelKey:   "true",
	}
}

func (h *SmdPatcher) handleDaemonSet(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver, version string) error {
	podSpec := h.buildPodSpec(version)
	labels := h.smdLabels()

	builder := k8sutil.NewDaemonSetBuilder(h.daemonSetName(), h.namespace)
	ds := builder.Build()

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		// Never adopt a DaemonSet labeled for a different instance (same
		// guard as the driver pool DaemonSets).
		if ds.ResourceVersion != "" {
			if existing := ds.Labels[driverManagerInstanceLabelKey]; existing != h.instanceName {
				return fmt.Errorf("DaemonSet %s/%s name collision: owned by instance %q, refusing to adopt for instance %q",
					h.namespace, h.daemonSetName(), existing, h.instanceName)
			}
		}
		builder.
			WithLabels(labels).
			WithLabelSelectors(labels).
			WithPodSpec(podSpec).
			// OnDelete: a template change never replaces a running smd pod.
			// Replacement rides the driver lifecycle instead — on every driver
			// pod start k8s-driver-manager evicts this node's smd pod via the
			// deploy-label flip and the recreated pod picks up the current
			// template, keeping smd and driver versions aligned per node.
			WithUpdateStrategy(appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.OnDeleteDaemonSetStrategyType,
			})
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile rbln-smd DaemonSet")
		return err
	}
	h.log.Info("Reconciled rbln-smd DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}

func (h *SmdPatcher) buildPodSpec(version string) *corev1.PodSpec {
	initContainer := h.buildDriverReadyInitContainer()

	// No command override: every rbln-smd image ships an ENTRYPOINT pointing at
	// its own binary, so leaving it unset keeps the operator out of the way when
	// that path moves (it did once already: /opt/rebellions/bin/rbln_daemon →
	// /usr/bin/rbln-smd) instead of requiring a lockstep operator release.
	smdContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Smd.Registry, h.desiredSpec.Smd.Image), version, h.desiredSpec.ImagePullPolicy).
		WithResources(corev1.ResourceRequirements{}, "250m", "40Mi").
		WithPorts([]corev1.ContainerPort{
			{
				Name:          smdPortName,
				ContainerPort: smdPort,
				Protocol:      corev1.ProtocolTCP,
			},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: smdVarRunVolumeName, MountPath: smdVarRunPath},
			{Name: hostSysVolumeName, MountPath: hostSysPath, ReadOnly: true},
			{Name: smdDebugVolumeName, MountPath: smdDebugPath},
			{Name: smdLogVolumeName, MountPath: smdLogPath},
		}).
		Build()

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithHostNetwork(true).
		WithNodeSelector(h.smdNodeSelector()).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(h.desiredSpec.PriorityClassName).
		WithVolumes([]corev1.Volume{
			smdHostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			smdHostPathVolume(smdVarRunVolumeName, smdVarRunPath, corev1.HostPathDirectory),
			smdHostPathVolume(hostSysVolumeName, hostSysPath, corev1.HostPathDirectory),
			smdHostPathVolume(smdDebugVolumeName, smdDebugPath, corev1.HostPathDirectory),
			smdHostPathVolume(smdLogVolumeName, smdLogPath, corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{smdContainer}).
		WithTerminationGracePeriodSeconds(0).
		Build()
}

// buildDriverReadyInitContainer blocks the smd container until this node's
// driver container publishes its ready marker, ordering smd start after the
// driver install per node. It runs the driver-manager image: that image is
// already pulled on every driver-owned node by the driver DaemonSet and,
// unlike the smd image, is guaranteed to ship a shell.
func (h *SmdPatcher) buildDriverReadyInitContainer() *corev1.Container {
	marker := consts.ValidationsMountPath + "/" + driverCtrReadyFile
	return k8sutil.NewContainerBuilder().
		WithName(smdInitContainerName).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Manager.Registry, h.desiredSpec.Manager.Image), h.desiredSpec.Manager.Version, h.desiredSpec.Manager.ImagePullPolicy).
		WithCommands([]string{"sh", "-c"}).
		WithArgs([]string{"until [ -f " + marker + " ]; do echo waiting for the rbln driver to be ready; sleep 5; done"}).
		WithSecurityContext(&corev1.SecurityContext{Privileged: ptr(true)}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             consts.ValidationsVolumeName,
				MountPath:        consts.ValidationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
}

func smdHostPathVolume(name, path string, hostPathType corev1.HostPathType) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: path, Type: ptr(hostPathType),
		}},
	}
}
