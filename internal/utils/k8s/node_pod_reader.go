/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package k8sutil

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func NodeIdentityEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "spec.nodeName"}}},
		{Name: "OPERATOR_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
	}
}

// ReconcileNodePodReader grants node lookup and namespace-scoped pod listing.
// Shared service accounts (such as smd) retain all of their driver CR owners.
func ReconcileNodePodReader(
	ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object,
	namespace, serviceAccount, name string, controllerOwner bool,
) error {
	for _, object := range nodePodReaderObjects(namespace, serviceAccount, name) {
		desired := object.DeepCopyObject().(client.Object)
		_, err := controllerutil.CreateOrPatch(ctx, c, object, func() error {
			switch o := object.(type) {
			case *rbacv1.ClusterRole:
				o.Rules = desired.(*rbacv1.ClusterRole).Rules
			case *rbacv1.Role:
				o.Rules = desired.(*rbacv1.Role).Rules
			case *rbacv1.ClusterRoleBinding:
				d := desired.(*rbacv1.ClusterRoleBinding)
				o.RoleRef, o.Subjects = d.RoleRef, d.Subjects
			case *rbacv1.RoleBinding:
				d := desired.(*rbacv1.RoleBinding)
				o.RoleRef, o.Subjects = d.RoleRef, d.Subjects
			}
			if controllerOwner {
				return controllerutil.SetControllerReference(owner, object, scheme)
			}
			return controllerutil.SetOwnerReference(owner, object, scheme)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteNodePodReader(ctx context.Context, c client.Client, namespace, name string) error {
	for _, object := range nodePodReaderObjects(namespace, "", name) {
		if err := client.IgnoreNotFound(c.Delete(ctx, object)); err != nil {
			return err
		}
	}
	return nil
}

func nodePodReaderObjects(namespace, serviceAccount, name string) []client.Object {
	subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: serviceAccount, Namespace: namespace}}
	return []client.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get"}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
			Subjects:   subjects,
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
			Subjects:   subjects,
		},
	}
}
