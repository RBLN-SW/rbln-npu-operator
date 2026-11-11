package components

import (
	"context"
	"slices"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	k8sutil "github.com/RBLN-SW/rbln-npu-operator/internal/utils/k8s"
)

const (
	deviceConfigManagerCommand      = "reconcile-partition-state"
	deviceConfigManagerLogLevelEnv  = "RBLN_DEVICE_CONFIG_MANAGER_LOG_LEVEL"
	deviceConfigManagerLogFormatEnv = "RBLN_DEVICE_CONFIG_MANAGER_LOG_FORMAT"
)

type deviceConfigManagerPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNDeviceConfigManagerSpec
	podDefaults *rblnv1beta1.PodDefaultsSpec
}

func NewDeviceConfigManagerPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.DeviceConfigManager)
	return &deviceConfigManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNDeviceConfigManagerName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigAll,
		},
		desiredSpec: &synced,
		podDefaults: cpSpec.PodDefaults,
	}
}

func (h *deviceConfigManagerPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.IsEnabled() {
		return nil
	}
	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx, owner); err != nil {
		return err
	}
	return h.handleDaemonSet(ctx, owner)
}

func (h *deviceConfigManagerPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.VDebug).Info("Cleaning up disabled component", "component", "Device Config Manager")
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}); err != nil {
		return err
	}
	if err := h.deleteIfExists(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}

func (h *deviceConfigManagerPatcher) handleClusterRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	clusterRole := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.name}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, clusterRole, func() error {
		clusterRole.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{"resource.k8s.io"}, Resources: []string{"resourceslices"}, Verbs: []string{"list"}},
			{APIGroups: []string{"rebellions.ai"}, Resources: []string{"rblndrivers"}, Verbs: []string{"list"}},
			{APIGroups: []string{"resource.k8s.io"}, Resources: []string{"resourceclaims"}, Verbs: []string{"get"}},
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"nodes/status"},
				Verbs:     []string{"patch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/eviction"},
				Verbs:     []string{"create"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"get", "list", "watch"},
			},
		}
		return ctrl.SetControllerReference(owner, clusterRole, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Device Config Manager ClusterRole")
		return err
	}
	k8sutil.LogReconcileResult(h.log, "Reconciled Device Config Manager ClusterRole", res, "name", clusterRole.Name)
	return nil
}

func (h *deviceConfigManagerPatcher) handleClusterRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     h.name,
		}
		binding.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.name,
				Namespace: h.namespace,
			},
		}
		return ctrl.SetControllerReference(owner, binding, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Device Config Manager ClusterRoleBinding")
		return err
	}
	k8sutil.LogReconcileResult(h.log, "Reconciled Device Config Manager ClusterRoleBinding", res, "name", binding.Name)
	return nil
}

func (h *deviceConfigManagerPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	// Only driver readiness: toolkit and partition readiness depend on work
	// this manager performs and would create a circular startup dependency.
	initContainer := buildGateInitContainer(consts.RBLNDeviceConfigManagerName, owner.Spec.Validator)
	pullSecrets := slices.Clone(h.desiredSpec.ImagePullSecrets)
	for _, secret := range owner.Spec.Validator.ImagePullSecrets {
		if !slices.Contains(pullSecrets, secret) {
			pullSecrets = append(pullSecrets, secret)
		}
	}
	defaultWorkload := owner.Spec.WorkloadType
	if defaultWorkload == "" {
		defaultWorkload = consts.RBLNWorkloadConfigContainer
	}
	env := mergeEnvVars(
		[]corev1.EnvVar{
			{Name: "DEFAULT_WORKLOAD_TYPE", Value: defaultWorkload},
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1", FieldPath: "spec.nodeName",
				}},
			},
			{
				Name: "OPERATOR_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1", FieldPath: "metadata.namespace",
				}},
			},
		},
		h.desiredSpec.Env,
		loggingEnvVars(h.desiredSpec.Logging, deviceConfigManagerLogLevelEnv, deviceConfigManagerLogFormatEnv),
	)

	managerContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithArgs([]string{deviceConfigManagerCommand}).
		WithEnvs(env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		// VF provisioning uses SMD, but PCI bind/unbind and host module
		// loading are performed directly by device-config-manager.
		WithSecurityContext(&corev1.SecurityContext{
			Privileged:     ptr(true),
			RunAsUser:      ptr(int64(0)),
			SELinuxOptions: &corev1.SELinuxOptions{Level: "s0"},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
			{Name: "host-sys", MountPath: "/sys"},
			{
				Name:             "host-root",
				MountPath:        "/host",
				ReadOnly:         true,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithHostNetwork(true).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.device-config-manager": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(pullSecrets).
		WithPriorityClassName(podDefaultsPriorityClassName(h.podDefaults)).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
			hostPathVolume("host-sys", "/sys", corev1.HostPathDirectory),
			hostPathVolume("host-root", "/", corev1.HostPathDirectory),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{managerContainer}).
		Build()
}

func (h *deviceConfigManagerPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	sel := selectorLabels(consts.RBLNDeviceConfigManagerName)

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()

	res, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		builder.
			WithLabelSelectors(sel).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(podSpec)
		applyUpdateStrategy(ds, h.desiredSpec.UpdateStrategy)
		return ctrl.SetControllerReference(owner, ds, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Device Config Manager DaemonSet")
		return err
	}
	k8sutil.LogReconcileResult(h.log, "Reconciled Device Config Manager DaemonSet", res, "namespace", ds.Namespace, "name", ds.Name)
	return nil
}
