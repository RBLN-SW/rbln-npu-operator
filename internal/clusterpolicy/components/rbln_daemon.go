package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.RBLNDaemonSpec
	name             string
	namespace        string
	openshiftVersion string
}

func NewRBLNDaemonPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	patcher := &rblnDaemonPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             consts.RBLNDaemonName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
	}

	synced := syncSpec(cpSpec, cpSpec.RBLNDaemon)
	patcher.desiredSpec = &synced
	return patcher
}

func (h *rblnDaemonPatcher) IsEnabled() bool {
	if h.desiredSpec == nil {
		return false
	}
	return h.desiredSpec.IsEnabled()
}

func (h *rblnDaemonPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.desiredSpec.IsEnabled() {
		return nil
	}

	if err := h.handleServiceAccount(ctx, owner); err != nil {
		return err
	}
	if h.openshiftVersion != "" {
		if err := h.handleRole(ctx, owner); err != nil {
			return err
		}
		if err := h.handleRoleBinding(ctx, owner); err != nil {
			return err
		}
	}
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}
	if err := h.handleService(ctx, owner); err != nil {
		return err
	}
	return nil
}

func (h *rblnDaemonPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: RBLN Daemon is disabled. Remove all RBLN Daemon resources")
	if err := h.client.Delete(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if h.openshiftVersion != "" {
		if err := h.client.Delete(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      h.name,
				Namespace: h.namespace,
			},
		}); err != nil && !kapierrors.IsNotFound(err) {
			return err
		}
		if err := h.client.Delete(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      h.name,
				Namespace: h.namespace,
			},
		}); err != nil && !kapierrors.IsNotFound(err) {
			return err
		}
	}
	if err := h.client.Delete(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (h *rblnDaemonPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
	var ds appsv1.DaemonSet
	if err := h.client.Get(ctx, types.NamespacedName{Name: h.name, Namespace: h.namespace}, &ds); err != nil {
		return []metav1.Condition{{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionFalse,
			Reason:             DaemonSetNotFound,
			Message:            fmt.Sprintf("DaemonSet %s/%s could not be found: %v", h.namespace, h.name, err),
			LastTransitionTime: metav1.Now(),
		}}, nil
	}

	ready := ds.Status.DesiredNumberScheduled > 0 &&
		ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberUnavailable == 0

	observedGen := ds.GetGeneration()

	if !ready {
		return []metav1.Condition{
			{
				Type:   DaemonSetReady,
				Status: metav1.ConditionFalse,
				Reason: DaemonSetPodsNotReady,
				Message: fmt.Sprintf(
					"DaemonSet %s/%s is progressing: %d of %d pods are Ready (%d unavailable)",
					h.namespace,
					h.name,
					ds.Status.NumberReady,
					ds.Status.DesiredNumberScheduled,
					ds.Status.NumberUnavailable,
				),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: observedGen,
			},
		}, nil
	}

	return []metav1.Condition{
		{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionTrue,
			Reason:             DaemonSetAllPodsReady,
			Message:            fmt.Sprintf("All pods in DaemonSet %s/%s are running", h.namespace, h.name),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: observedGen,
		},
	}, nil
}

func (h *rblnDaemonPatcher) ComponentName() string {
	return h.name
}

func (h *rblnDaemonPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *rblnDaemonPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon ServiceAccount")
		return err
	}
	h.log.Info("Reconciled RBLNDaemon ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *rblnDaemonPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBuilder(h.name, h.namespace)
	rb := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, rb, func() error {
		rb = builder.
			WithRules(rbacv1.PolicyRule{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			}).
			WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon Role")
		return err
	}
	h.log.Info("Reconciled RBLNDaemon Role", "namespace", rb.Namespace, "name", rb.Name, "result", roleRes)
	return nil
}

func (h *rblnDaemonPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBindingBuilder(h.name, h.namespace)
	rbb := builder.Build()

	roleBindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, rbb, func() error {
		rbb = builder.
			WithRoleRef(rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     h.name,
			}).
			WithSubjects(rbacv1.Subject{
				Kind:      "ServiceAccount",
				Name:      h.name,
				Namespace: h.namespace,
			}).
			WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon RoleBinding")
		return err
	}
	h.log.Info("Reconciled RBLNDaemon RoleBinding", "namespace", rbb.Namespace, "name", rbb.Name, "result", roleBindingRes)
	return nil
}

func (h *rblnDaemonPatcher) handleService(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceBuilder(h.name, h.namespace)
	svc := builder.Build()
	labelsMap := map[string]string{"app": h.name}
	svcRes, err := controllerutil.CreateOrPatch(ctx, h.client, svc, func() error {
		svc = builder.
			WithLabels(labelsMap).
			WithSelector(labelsMap).
			WithPorts([]corev1.ServicePort{
				{
					Name: rblnDaemonPortName,
					Port: rblnDaemonDefaultHostPort,
				},
			}).
			WithOwner(owner, h.scheme).
			Build()
		svc.Spec.InternalTrafficPolicy = ptr(corev1.ServiceInternalTrafficPolicyLocal)
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon Service")
		return err
	}

	h.log.Info("Reconciled RBLNDaemon Service", "namespace", svc.Namespace, "name", svc.Name, "result", svcRes)
	return nil
}

func (h *rblnDaemonPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()
	labelsMap := map[string]string{"app": h.name}
	validatorSpec := owner.Spec.Validator

	initContainer := k8sutil.NewContainerBuilder().
		WithName("toolkit-validation").
		WithImage(ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{"sh", "-c"}).
		WithArgs([]string{"until [ -f " + validationsMountPath + "/toolkit-ready ]; do echo waiting for rbln container stack to be setup; sleep 5; done"}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             validationsVolumeName,
				MountPath:        validationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
	if validatorSpec.ImagePullPolicy == "" {
		initContainer.ImagePullPolicy = corev1.PullIfNotPresent
	}

	hostPort := h.desiredSpec.HostPort
	if hostPort == 0 {
		hostPort = rblnDaemonDefaultHostPort
	}

	daemonContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithCommands([]string{rblnDaemonCommand}).
		WithArgs(h.desiredSpec.Args).
		WithEnvs(h.desiredSpec.Env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:      rblnDaemonVarRunVolumeName,
				MountPath: rblnDaemonVarRunPath,
			},
			{
				Name:      rblnDaemonSysVolumeName,
				MountPath: rblnDaemonSysPath,
				ReadOnly:  true,
			},
			{
				Name:      rblnDaemonDebugVolumeName,
				MountPath: rblnDaemonDebugPath,
			},
			{
				Name:      rblnDaemonLogVolumeName,
				MountPath: rblnDaemonLogPath,
			},
		}).
		Build()
	daemonContainer.Ports = []corev1.ContainerPort{
		{
			Name:          rblnDaemonPortName,
			ContainerPort: rblnDaemonDefaultHostPort,
			HostPort:      hostPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(labelsMap).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(
				k8sutil.NewPodSpecBuilder().
					WithServiceAccountName(h.name).
					WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.rbln-daemon": "true"}).
					WithAffinity(h.desiredSpec.Affinity).
					WithTolerations(h.desiredSpec.Tolerations).
					WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
					WithPriorityClassName(h.desiredSpec.PriorityClassName).
					WithVolumes([]corev1.Volume{
						{
							Name: validationsVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: validationsMountPath,
									Type: ptr(corev1.HostPathDirectoryOrCreate),
								},
							},
						},
						{
							Name: rblnDaemonVarRunVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: rblnDaemonVarRunPath,
									Type: ptr(corev1.HostPathDirectory),
								},
							},
						},
						{
							Name: rblnDaemonSysVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: rblnDaemonSysPath,
									Type: ptr(corev1.HostPathDirectory),
								},
							},
						},
						{
							Name: rblnDaemonDebugVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: rblnDaemonDebugPath,
									Type: ptr(corev1.HostPathDirectory),
								},
							},
						},
						{
							Name: rblnDaemonLogVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: rblnDaemonLogPath,
									Type: ptr(corev1.HostPathDirectoryOrCreate),
								},
							},
						},
					}).
					WithInitContainers([]*corev1.Container{initContainer}).
					WithContainers([]*corev1.Container{daemonContainer}).
					WithTerminationGracePeriodSeconds(0).
					Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDaemon DaemonSet")
		return err
	}

	h.log.Info("Reconciled RBLNDaemon DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}
