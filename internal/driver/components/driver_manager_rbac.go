package components

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

func (h *driverManagerPatcher) handleClusterRole(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}
	res, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/eviction"},
				Verbs:     []string{"create"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"get"},
			},
		}
		return controllerutil.SetOwnerReference(owner, role, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile ClusterRole", "name", h.name)
		return err
	}
	h.log.Info("Reconciled ClusterRole", "name", role.Name, "result", res)
	return nil
}

func (h *driverManagerPatcher) handleClusterRoleBinding(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: h.name},
	}
	res, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     h.name,
		}
		binding.Subjects = []rbacv1.Subject{
			{Kind: rbacv1.ServiceAccountKind, Name: h.name, Namespace: h.namespace},
		}
		return controllerutil.SetOwnerReference(owner, binding, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile ClusterRoleBinding", "name", h.name)
		return err
	}
	h.log.Info("Reconciled ClusterRoleBinding", "name", binding.Name, "result", res)
	return nil
}
