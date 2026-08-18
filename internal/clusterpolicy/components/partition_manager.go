package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const partitionManagerCommand = "reconcile-partition-state"

type partitionManagerPatcher struct {
	basePatcher
	desiredSpec *rblnv1beta1.RBLNPartitionManagerSpec
	podDefaults *rblnv1beta1.PodDefaultsSpec
}

func NewPartitionManagerPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	synced := syncSpec(cpSpec, cpSpec.PartitionManager)
	return &partitionManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          synced.IsEnabled(),
			workloadType:     consts.RBLNWorkloadConfigContainer,
		},
		desiredSpec: &synced,
		podDefaults: cpSpec.PodDefaults,
	}
}

func (h *partitionManagerPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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

func (h *partitionManagerPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "Partition Manager")
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

func (h *partitionManagerPatcher) handleClusterRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	clusterRole := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.name}}

	res, err := controllerutil.CreateOrPatch(ctx, h.client, clusterRole, func() error {
		clusterRole.Rules = []rbacv1.PolicyRule{
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
		h.log.Error(err, "Failed to reconcile Partition Manager ClusterRole")
		return err
	}
	h.log.Info("Reconciled Partition Manager ClusterRole", "name", clusterRole.Name, "result", res)
	return nil
}

func (h *partitionManagerPatcher) handleClusterRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		h.log.Error(err, "Failed to reconcile Partition Manager ClusterRoleBinding")
		return err
	}
	h.log.Info("Reconciled Partition Manager ClusterRoleBinding", "name", binding.Name, "result", res)
	return nil
}

func (h *partitionManagerPatcher) buildPodSpec(owner *rblnv1beta1.RBLNClusterPolicy) *corev1.PodSpec {
	initContainer := buildGateInitContainer(consts.RBLNPartitionManagerName, owner.Spec.Validator)

	env := mergeEnvVars(
		[]corev1.EnvVar{
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
	)

	managerContainer := k8sutil.NewContainerBuilder().
		WithName(h.name).
		WithImage(k8sutil.ComposeImageReference(h.desiredSpec.Registry, h.desiredSpec.Image), h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
		WithArgs([]string{partitionManagerCommand}).
		WithEnvs(env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
		}).
		Build()

	return k8sutil.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithHostNetwork(true).
		WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.partition-manager": "true"}).
		WithAffinity(h.desiredSpec.Affinity).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(podDefaultsPriorityClassName(h.podDefaults)).
		WithVolumes([]corev1.Volume{
			hostPathVolume(consts.ValidationsVolumeName, consts.ValidationsMountPath, corev1.HostPathDirectoryOrCreate),
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{managerContainer}).
		Build()
}

func (h *partitionManagerPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	podSpec := h.buildPodSpec(owner)

	sel := selectorLabels(consts.RBLNPartitionManagerName)

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
		h.log.Error(err, "Failed to reconcile Partition Manager DaemonSet")
		return err
	}
	h.log.Info("Reconciled Partition Manager DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", res)
	return nil
}
