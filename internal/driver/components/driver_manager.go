package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// ─── Struct ──────────────────────────────────────────────────────────────────

type driverManagerPatcher struct {
	basePatcher
	desiredSpec *rebellionsaiv1alpha1.RBLNDriverSpec
}

func NewDriverManagerPatcher(
	client client.Client,
	log logr.Logger,
	namespace string,
	driver *rebellionsaiv1alpha1.RBLNDriver,
	scheme *runtime.Scheme,
	openshiftVersion string,
) (DriverPatcher, error) {
	if driver == nil {
		return nil, fmt.Errorf("driver is nil")
	}
	return &driverManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             driverManagerName,
			instanceName:     driver.Name,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          true,
		},
		desiredSpec: &driver.Spec,
	}, nil
}

// ─── Interface methods ───────────────────────────────────────────────────────

func (h *driverManagerPatcher) Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
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
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}

	nodePools, err := getNodePools(ctx, h.client, h.desiredSpec.NodeSelector)
	if err != nil {
		return err
	}
	if len(nodePools) == 0 {
		h.log.Info("no nodes matching the given selector for driver manager; cleaning up all DaemonSets", "instance", h.instanceName)
		return h.cleanUpStaleDaemonSets(ctx, nil)
	}
	for _, nodePool := range nodePools {
		if err := h.handleDaemonSet(ctx, owner, nodePool); err != nil {
			return err
		}
	}

	if err := h.cleanUpStaleDaemonSets(ctx, nodePools); err != nil {
		return err
	}

	return nil
}

func (h *driverManagerPatcher) IsReady(ctx context.Context) error {
	dsList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, dsList,
		client.InNamespace(h.namespace),
		client.MatchingLabels(map[string]string{
			driverManagerAppLabelKey:      h.name,
			driverManagerInstanceLabelKey: h.instanceName,
		}),
	); err != nil {
		return fmt.Errorf("list driver DaemonSets: %w", err)
	}

	if len(dsList.Items) == 0 {
		return nil
	}

	for i := range dsList.Items {
		ds := &dsList.Items[i]
		ready := ds.Status.DesiredNumberScheduled > 0 &&
			ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
			ds.Status.NumberUnavailable == 0
		if !ready {
			return fmt.Errorf("DaemonSet %s/%s is progressing: %d of %d pods are Ready (%d unavailable)",
				ds.Namespace, ds.Name,
				ds.Status.NumberReady, ds.Status.DesiredNumberScheduled, ds.Status.NumberUnavailable)
		}
	}

	return nil
}

func (h *driverManagerPatcher) CleanUp(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	h.log.V(consts.LogLevelDebug).Info("Cleaning up disabled component", "component", "Driver Manager")

	// Delete all DaemonSets for this instance.
	dsList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, dsList, client.InNamespace(h.namespace), client.MatchingLabels(map[string]string{
		driverManagerAppLabelKey:      h.name,
		driverManagerInstanceLabelKey: h.instanceName,
	})); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	for _, ds := range dsList.Items {
		if err := h.deleteIfExists(ctx, &ds); err != nil {
			return err
		}
	}

	// Cluster-scoped resources (ClusterRole, ClusterRoleBinding) and shared
	// namespaced resources (ServiceAccount, Role, RoleBinding) carry
	// ownerReferences to each RBLNDriver instance. Kubernetes GC will only
	// delete them once all owners are gone, so no manual deletion is needed.
	if err := h.deleteIfExists(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: h.startupProbeConfigMapName(), Namespace: h.namespace},
	}); err != nil {
		return err
	}
	return nil
}
