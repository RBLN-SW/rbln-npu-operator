package components

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

// handleDaemonSet creates or updates one pool's DaemonSet. deferCreate skips
// creating one that does not exist yet, for the pass where a stale DaemonSet
// was kept: the kept one still schedules onto this pool's nodes, and two driver
// installers on one host is exactly what the reap ordering exists to prevent.
// Updating an existing DaemonSet stays safe -- its pods already own those
// nodes, and OnDelete means the update replaces none of them.
func (h *driverManagerPatcher) handleDaemonSet(
	ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver, pool nodePool, imagePath string, deferCreate bool,
) error {
	podSpec := h.buildDriverPodSpec(pool, imagePath)

	ds := k8sutil.NewDaemonSetBuilder(h.instanceName+"-"+pool.name, h.namespace).
		WithLabels(h.driverManagerLabels(pool)).
		WithLabelSelectors(h.driverManagerLabels(pool)).
		WithAnnotations(h.desiredSpec.Annotations).
		WithPodSpec(podSpec).
		WithUpdateStrategy(appsv1.DaemonSetUpdateStrategy{
			Type: appsv1.OnDeleteDaemonSetStrategyType,
		}).
		Build()

	if err := ctrl.SetControllerReference(owner, ds, h.scheme); err != nil {
		return err
	}

	driverConfigDigest := k8sutil.GetObjectHash(ds.Spec.Template.Spec.Containers)
	ds.Spec.Template.Spec.InitContainers[0].Env = upsertEnvVar(
		ds.Spec.Template.Spec.InitContainers[0].Env,
		corev1.EnvVar{Name: driverConfigDigestEnv, Value: driverConfigDigest},
	)
	ds.Annotations = k8sutil.MergeMaps(ds.Annotations, map[string]string{
		driverLastAppliedHashAnnotation: driverConfigDigest,
	})

	current, err := h.getDaemonSet(ctx, ds.Name)
	if err != nil {
		if !kapierrors.IsNotFound(err) {
			return err
		}
		if deferCreate {
			h.log.V(consts.LogLevelDebug).Info("deferring pool DaemonSet creation; a stale DaemonSet was kept this pass",
				"pool", pool.name)
			return nil
		}
		if err := h.client.Create(ctx, ds); err != nil {
			h.log.Error(err, "Failed to create DaemonSet")
			return err
		}
		h.log.Info("Reconciled DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", "created")
		return nil
	}

	// Never adopt or update a DaemonSet labeled for a different instance: the
	// hash check below has no idea this object belongs to someone else, and
	// would happily repoint its ownerReferences, instance label, and selector
	// onto this instance's driver.
	if existingInstance := current.Labels[driverManagerInstanceLabelKey]; existingInstance != h.instanceName {
		return fmt.Errorf("DaemonSet %s/%s name collision: owned by instance %q, refusing to adopt for instance %q",
			ds.Namespace, ds.Name, existingInstance, h.instanceName)
	}

	if h.shouldSkipDaemonSetUpdateByDriverHash(current, driverConfigDigest) {
		return nil
	}

	ds.SetResourceVersion(current.GetResourceVersion())
	ds.SetFinalizers(current.GetFinalizers())
	if err := h.client.Update(ctx, ds); err != nil {
		h.log.Error(err, "Failed to update DaemonSet")
		return err
	}
	h.log.Info("Reconciled DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", "updated")
	return nil
}

func (h *driverManagerPatcher) getDaemonSet(ctx context.Context, name string) (*appsv1.DaemonSet, error) {
	current := &appsv1.DaemonSet{}
	err := h.client.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, current)
	if err != nil {
		return nil, err
	}
	return current, nil
}

func (h *driverManagerPatcher) shouldSkipDaemonSetUpdateByDriverHash(current *appsv1.DaemonSet, driverConfigDigest string) bool {
	currentHash := current.Annotations[driverLastAppliedHashAnnotation]
	if currentHash == "" {
		currentHash = k8sutil.GetObjectHash(current.Spec.Template.Spec.Containers)
	}

	if currentHash == driverConfigDigest {
		h.log.Info(
			"Skip DaemonSet update: driver container unchanged",
			"namespace", current.Namespace,
			"name", current.Name,
			"hash", driverConfigDigest,
		)
		return true
	}

	return false
}

func (h *driverManagerPatcher) driverManagerLabels(pool nodePool) map[string]string {
	return map[string]string{
		driverManagerAppNameLabelKey:  consts.OperatorName,
		driverManagerAppLabelKey:      h.name,
		driverManagerNodePoolLabelKey: pool.name,
		driverManagerInstanceLabelKey: h.instanceName,
	}
}

// findStaleDaemonSets returns this instance's DaemonSets whose node pool no
// longer has any matching nodes (e.g. after a kernel upgrade, or a pool
// rename). desiredPoolNames holds pool-name label values -- the same key each
// DaemonSet is matched on below -- so callers pass the set directly rather than
// a []nodePool; a nil set makes every DaemonSet this instance owns stale.
//
// Split from the delete so callers can find out whether anything is stale
// before deciding to reap: while a pool is unrenderable, a stale DaemonSet can
// be the only driver its nodes have.
func (h *driverManagerPatcher) findStaleDaemonSets(
	ctx context.Context,
	desiredPoolNames map[string]struct{},
) ([]*appsv1.DaemonSet, error) {
	existingDSList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, existingDSList,
		client.InNamespace(h.namespace),
		client.MatchingLabels(map[string]string{
			driverManagerAppLabelKey:      h.name,
			driverManagerInstanceLabelKey: h.instanceName,
		}),
	); err != nil {
		return nil, fmt.Errorf("list existing driver DaemonSets: %w", err)
	}

	stale := make([]*appsv1.DaemonSet, 0, len(existingDSList.Items))
	for i := range existingDSList.Items {
		ds := &existingDSList.Items[i]
		if ds.GetDeletionTimestamp() != nil {
			// Already being deleted (e.g. stuck Terminating behind a
			// third-party finalizer) -- re-issuing Delete on it every pass
			// is a harmless but pointless API call.
			continue
		}
		if _, desired := desiredPoolNames[ds.Labels[driverManagerNodePoolLabelKey]]; desired {
			continue
		}
		stale = append(stale, ds)
	}
	return stale, nil
}

func (h *driverManagerPatcher) reapDaemonSets(ctx context.Context, stale []*appsv1.DaemonSet) error {
	for _, ds := range stale {
		h.log.Info("Deleting stale DaemonSet for removed node pool",
			"namespace", ds.Namespace,
			"name", ds.Name,
			"nodePool", ds.Labels[driverManagerNodePoolLabelKey],
		)
		// Foreground: a background delete drops the object immediately
		// while its pods are still terminating, so an in-progress reap
		// reads as finished. Not an interlock -- Patch creates the
		// successor in this same call, so a pool rename still overlaps
		// the old and new driver pods on a node.
		if err := h.client.Delete(ctx, ds,
			client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !kapierrors.IsNotFound(err) {
			return fmt.Errorf("delete stale DaemonSet %s/%s: %w", ds.Namespace, ds.Name, err)
		}
	}
	return nil
}

func (h *driverManagerPatcher) startupProbeConfigMapName() string {
	return fmt.Sprintf("%s-%s", h.name, startupProbeConfigMapSuffix)
}

func upsertEnvVar(envs []corev1.EnvVar, target corev1.EnvVar) []corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == target.Name {
			envs[i] = target
			return envs
		}
	}
	return append(envs, target)
}
