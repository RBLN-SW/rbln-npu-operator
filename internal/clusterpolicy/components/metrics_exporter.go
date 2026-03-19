package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/apps/v1"
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

type metricsExporterPatcher struct {
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.RBLNMetricsExporterSpec
	name             string
	namespace        string
	openshiftVersion string
}

func NewMetricsExporterPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	patcher := &metricsExporterPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             cpSpec.BaseName + "-" + consts.RBLNMetricExporterName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
	}

	synced := syncSpec(cpSpec, cpSpec.MetricsExporter)
	patcher.desiredSpec = &synced
	return patcher
}

func (h *metricsExporterPatcher) IsEnabled() bool {
	if h.desiredSpec == nil {
		return false
	}

	return h.desiredSpec.IsEnabled()
}

func (h *metricsExporterPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.desiredSpec.IsEnabled() {
		return nil
	}

	// reconcile serviceaccount
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

	// reconcile daemonset
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}

	// reconcile service
	if err := h.handleService(ctx, owner); err != nil {
		return err
	}

	return nil
}

func (h *metricsExporterPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Metrics Exporter is disabled. Remove all Metrics Exporter resources")
	if err := h.client.Delete(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name + "-service",
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	if err := h.client.Delete(ctx, &v1.DaemonSet{
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

func (h *metricsExporterPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
	var ds v1.DaemonSet
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

func (h *metricsExporterPatcher) ComponentName() string {
	return h.name
}

func (h *metricsExporterPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *metricsExporterPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNMetricsExporter ServiceAccount")
		return err
	}
	h.log.Info("Reconciled RBLNMetricsExporter ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *metricsExporterPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile RBLNMetricsExporter Role")
		return err
	}
	h.log.Info("Reconciled RBLNMetricsExporter Role", "namespace", rb.Namespace, "name", rb.Name, "result", roleRes)
	return nil
}

func (h *metricsExporterPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile RBLNMetricsExporter RoleBinding")
		return err
	}
	h.log.Info("Reconciled RBLNMetricsExporter RoleBinding", "namespace", rbb.Namespace, "name", rbb.Name, "result", roleBindingRes)
	return nil
}

func (h *metricsExporterPatcher) handleService(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceBuilder(h.name+"-service", h.namespace)
	svc := builder.Build()
	labelsMap := map[string]string{
		"app": h.name,
	}
	svcRes, err := controllerutil.CreateOrPatch(ctx, h.client, svc, func() error {
		svc = builder.
			WithAnnotations(map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/path":   "/metrics",
				"prometheus.io/port":   "9090",
			}).
			WithLabels(labelsMap).
			WithSelector(labelsMap).
			WithPorts([]corev1.ServicePort{
				{
					Name: "http",
					Port: 9090,
				},
			}).
			WithOwner(cp, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNMetricsExporter Service")
		return err
	}

	h.log.Info("Reconciled RBLNMetricsExporter Service", "namespace", svc.Namespace, "name", svc.Name, "result", svcRes)
	return nil
}

func (h *metricsExporterPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()
	labelsMap := map[string]string{
		"app": h.name,
	}
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
	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(labelsMap).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(
				k8sutil.NewPodSpecBuilder().
					WithServiceAccountName(h.name).
					WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.metrics-exporter": "true"}).
					WithAffinity(h.desiredSpec.Affinity).
					WithTolerations(h.desiredSpec.Tolerations).
					WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
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
							Name: "pod-resources",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/pod-resources",
									Type: ptr(corev1.HostPathDirectory),
								},
							},
						},
						{
							Name: "sysfs",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/sys",
									Type: ptr(corev1.HostPathDirectory),
								},
							},
						},
					}).
					WithInitContainers([]*corev1.Container{initContainer}).
					WithContainers([]*corev1.Container{
						k8sutil.NewContainerBuilder().
							WithName(h.name).
							WithImage(ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
							WithVolumeMounts([]corev1.VolumeMount{
								{
									Name:      "pod-resources",
									MountPath: "/var/lib/kubelet/pod-resources",
									ReadOnly:  true,
								},
								{
									Name:      "sysfs",
									MountPath: "/sys",
									ReadOnly:  true,
								},
							}).
							WithEnvs([]corev1.EnvVar{
								{
									Name: "NODE_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "status.hostIP",
										},
									},
								},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "RBLN_METRICS_EXPORTER_RBLN_DAEMON_URL",
									Value: "http://$(NODE_IP):50051",
								},
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
					Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNMetricsExporter DaemonSet")
		return err
	}

	h.log.Info("Reconciled RBLNMetricsExporter DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}
