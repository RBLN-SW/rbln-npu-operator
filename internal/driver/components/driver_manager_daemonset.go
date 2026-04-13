package components

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func (h *driverManagerPatcher) handleDaemonSet(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver, pool nodePool) error {
	podSpec, err := h.buildDriverPodSpec(pool)
	if err != nil {
		return err
	}

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
		if err := h.client.Create(ctx, ds); err != nil {
			h.log.Error(err, "Failed to create DaemonSet")
			return err
		}
		h.log.Info("Reconciled DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", "created")
		return nil
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
	if current == nil {
		return false
	}

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

// cleanUpStaleDaemonSets removes DaemonSets that belong to node pools which
// no longer have any matching nodes (e.g. after a kernel upgrade).
func (h *driverManagerPatcher) cleanUpStaleDaemonSets(ctx context.Context, desiredPools []nodePool) error {
	desiredPoolNames := make(map[string]struct{}, len(desiredPools))
	for _, pool := range desiredPools {
		desiredPoolNames[pool.name] = struct{}{}
	}

	existingDSList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, existingDSList,
		client.InNamespace(h.namespace),
		client.MatchingLabels(map[string]string{
			driverManagerAppLabelKey:      h.name,
			driverManagerInstanceLabelKey: h.instanceName,
		}),
	); err != nil {
		return fmt.Errorf("list existing driver DaemonSets: %w", err)
	}

	for i := range existingDSList.Items {
		ds := &existingDSList.Items[i]
		poolName := ds.Labels[driverManagerNodePoolLabelKey]
		if _, desired := desiredPoolNames[poolName]; !desired {
			h.log.Info("Deleting stale DaemonSet for removed node pool",
				"namespace", ds.Namespace,
				"name", ds.Name,
				"nodePool", poolName,
			)
			if err := h.deleteIfExists(ctx, ds); err != nil {
				return fmt.Errorf("delete stale DaemonSet %s/%s: %w", ds.Namespace, ds.Name, err)
			}
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
