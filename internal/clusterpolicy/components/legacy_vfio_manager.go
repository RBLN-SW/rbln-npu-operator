package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
)

// Existing policies survive the removal of spec.vfioManager, so owner garbage
// collection cannot retire its resources. Stop the old PCI binding controller
// before reconciling its replacement, even if device-config-manager is disabled.
// TODO(remove after two releases): retire with the legacy VFIO deploy label.
type legacyVFIOManagerPatcher struct{ basePatcher }

func NewLegacyVFIOManagerCleanup(c client.Client, log logr.Logger, namespace string, scheme *runtime.Scheme) Patcher {
	return &legacyVFIOManagerPatcher{basePatcher: basePatcher{
		client: c, log: log, namespace: namespace, scheme: scheme, name: "rbln-vfio-manager",
	}}
}

func (h *legacyVFIOManagerPatcher) Patch(context.Context, *rblnv1beta1.RBLNClusterPolicy) error {
	return nil
}

func (h *legacyVFIOManagerPatcher) CleanUp(ctx context.Context, _ *rblnv1beta1.RBLNClusterPolicy) error {
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace}}
	if err := h.client.Delete(ctx, ds, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Get(ctx, client.ObjectKeyFromObject(ds), ds); err == nil {
		return fmt.Errorf("waiting for legacy VFIO manager %s/%s and its pods to terminate", h.namespace, h.name)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	for _, obj := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: h.name + "-config", Namespace: h.namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: h.name}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: h.name}},
	} {
		if err := h.deleteIfExists(ctx, obj); err != nil {
			return err
		}
	}
	return h.deleteServiceAccount(ctx)
}
