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
	apiReader client.Reader,
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
			apiReader:        apiReader,
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
	pools, err := h.PoolStatuses(ctx)
	if err != nil {
		return err
	}
	for _, p := range pools {
		if p.State != rebellionsaiv1alpha1.DriverPoolStateReady {
			return fmt.Errorf("driver pool %q is progressing: %d/%d pods ready",
				p.Name, p.Ready, p.Desired)
		}
	}
	return nil
}

// PoolStatuses returns an empty slice when no candidate nodes carry the
// driver-deploy label or NFD has not labeled them yet; callers interpret
// that as "nothing to do" rather than a failure.
//
// Uses apiReader (uncached) so a Delete issued earlier in the same
// reconcile by cleanUpStaleDaemonSets is observed here, not the
// still-cached pre-delete view.
func (h *driverManagerPatcher) PoolStatuses(ctx context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, error) {
	dsList := &appsv1.DaemonSetList{}
	if err := h.apiReader.List(ctx, dsList,
		client.InNamespace(h.namespace),
		client.MatchingLabels(map[string]string{
			driverManagerAppLabelKey:      h.name,
			driverManagerInstanceLabelKey: h.instanceName,
		}),
	); err != nil {
		return nil, fmt.Errorf("list driver DaemonSets: %w", err)
	}

	pools := make([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, 0, len(dsList.Items))
	for i := range dsList.Items {
		ds := &dsList.Items[i]
		desired := ds.Status.DesiredNumberScheduled
		ready := ds.Status.NumberReady

		state := rebellionsaiv1alpha1.DriverPoolStateReady
		if desired == 0 || ready != desired || ds.Status.NumberUnavailable > 0 {
			state = rebellionsaiv1alpha1.DriverPoolStateProgressing
		}

		pools = append(pools, rebellionsaiv1alpha1.RBLNDriverPoolStatus{
			Name:    ds.Name,
			Desired: desired,
			Ready:   ready,
			State:   state,
		})
	}
	return pools, nil
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
