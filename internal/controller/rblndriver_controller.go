/*
Copyright 2025.

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

package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/scope"
	"github.com/rebellions-sw/rbln-npu-operator/internal/validator"
)

// RBLNDriverReconciler reconciles a RBLNDriver object
type RBLNDriverReconciler struct {
	client.Client
	Log                   logr.Logger
	Scheme                *runtime.Scheme
	ClusterInfo           *ClusterInfo
	nodeSelectorValidator validator.NodeSelectorValidator
}

const (
	driverNodeDeployLabelKey = "rebellions.ai/npu.deploy.driver"
	nfdOSReleaseIDLabelKey   = "feature.node.kubernetes.io/system-os_release.ID"
	nfdOSVersionIDLabelKey   = "feature.node.kubernetes.io/system-os_release.VERSION_ID"
	nfdKernelLabelKey        = "feature.node.kubernetes.io/kernel-version.full"
)

// +kubebuilder:rbac:groups=rebellions.ai,resources=rblndrivers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblndrivers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblndrivers/finalizers,verbs=update
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts;nodes;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=use,resourceNames=privileged

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the RBLNDriver object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *RBLNDriverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling RBLNDriver", "name", req.Name)

	instance := &rebellionsaiv1alpha1.RBLNDriver{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if kapierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		wrappedErr := fmt.Errorf("error getting RBLNDriver object: %w", err)
		r.Log.Error(err, "error getting RBLNDriver object")
		r.setDriverStatusError(ctx, instance, wrappedErr)
		return ctrl.Result{}, wrappedErr
	}

	// Get the singleton RBLNClusterPolicy object in the cluster.
	clusterPolicyList := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := r.List(ctx, clusterPolicyList); err != nil {
		wrappedErr := fmt.Errorf("error getting RBLNClusterPolicy list: %w", err)
		r.Log.Error(err, "error getting RBLNClusterPolicy list")
		r.setDriverStatusError(ctx, instance, wrappedErr)
		return ctrl.Result{}, wrappedErr
	}

	if len(clusterPolicyList.Items) == 0 {
		r.Log.Info("RBLNClusterPolicy not found yet; skipping driver reconcile")
		r.setDriverStatusNotReady(ctx, instance, "MissingClusterPolicy", "RBLNClusterPolicy not found in the cluster")
		return ctrl.Result{}, nil
	}
	clusterPolicyInstance := clusterPolicyList.Items[0]

	nodeSelectorValidator := r.nodeSelectorValidator
	if nodeSelectorValidator == nil {
		nodeSelectorValidator = validator.NewNodeSelectorValidator(r.Client)
		r.nodeSelectorValidator = nodeSelectorValidator
	}
	if err := nodeSelectorValidator.Validate(ctx, instance); err != nil {
		r.Log.Info("WARNING: nodeSelector validation failed; skip reconcile", "name", req.Name, "error", err.Error())
		r.setDriverStatusError(ctx, instance, err)
		return ctrl.Result{}, nil
	}

	openshiftVersion := ""
	if r.ClusterInfo != nil {
		openshiftVersion = r.ClusterInfo.OpenshiftVersion
	}
	driverScope, err := scope.NewRBLNDriverScope(ctx, r.Client, r.Log, r.Scheme, instance, &clusterPolicyInstance, openshiftVersion)
	if err != nil {
		r.Log.Error(err, "failed to initialize RBLNDriver scope")
		r.setDriverStatusError(ctx, instance, err)
		return ctrl.Result{}, err
	}

	if err := driverScope.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "failed to patch driver manager resources")
		r.setDriverStatusError(ctx, instance, err)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RBLNDriverReconciler) setDriverStatusError(ctx context.Context, instance *rebellionsaiv1alpha1.RBLNDriver, err error) {
	instance.Status.State = rebellionsaiv1alpha1.DriverStateNotReady
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:   conditions.ConditionReady,
		Status: metav1.ConditionFalse,
		Reason: conditions.ConditionError,
	})
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:    conditions.ConditionError,
		Status:  metav1.ConditionTrue,
		Reason:  conditions.ReconcileFailed,
		Message: err.Error(),
	})
	if statusErr := r.Status().Update(ctx, instance); statusErr != nil {
		r.Log.Error(statusErr, "failed to update RBLNDriver status")
	}
}

func (r *RBLNDriverReconciler) setDriverStatusNotReady(ctx context.Context, instance *rebellionsaiv1alpha1.RBLNDriver, reason string, message string) {
	updated := false
	if instance.Status.State != rebellionsaiv1alpha1.DriverStateNotReady {
		instance.Status.State = rebellionsaiv1alpha1.DriverStateNotReady
		updated = true
	}

	readyCondition := metav1.Condition{
		Type:    conditions.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	}
	if shouldUpdateCondition(&instance.Status.Conditions, readyCondition) {
		updated = true
	}

	if updated {
		if statusErr := r.Status().Update(ctx, instance); statusErr != nil {
			r.Log.Error(statusErr, "failed to update RBLNDriver status")
		}
	}
}

func shouldUpdateCondition(conditionsList *[]metav1.Condition, desired metav1.Condition) bool {
	existing := meta.FindStatusCondition(*conditionsList, desired.Type)
	if existing != nil &&
		existing.Status == desired.Status &&
		existing.Reason == desired.Reason &&
		existing.Message == desired.Message {
		return false
	}
	meta.SetStatusCondition(conditionsList, desired)
	return true
}

// SetupWithManager sets up the controller with the Manager.
func (r *RBLNDriverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.nodeSelectorValidator = validator.NewNodeSelectorValidator(mgr.GetClient())

	mapFn := func(ctx context.Context, _ client.Object) []reconcile.Request {
		list := &rebellionsaiv1alpha1.RBLNDriverList{}
		if err := mgr.GetClient().List(ctx, list); err != nil {
			r.Log.Error(err, "unable to list RBLNDriver resources for RBLNClusterPolicy event")
			return nil
		}
		requests := make([]reconcile.Request, 0, len(list.Items))
		for _, driver := range list.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKey{Name: driver.GetName()},
			})
		}
		return requests
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&rebellionsaiv1alpha1.RBLNDriver{}).
		Owns(&appsv1.DaemonSet{}).
		Watches(&rblnv1beta1.RBLNClusterPolicy{}, handler.EnqueueRequestsFromMapFunc(mapFn)).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(mapFn),
			builder.WithPredicates(r.driverRelevantNodeLabelUpdated()),
		).
		Complete(r)
}

func (r *RBLNDriverReconciler) driverRelevantNodeLabelUpdated() predicate.Funcs {
	interested := []string{
		driverNodeDeployLabelKey,
		nfdOSReleaseIDLabelKey,
		nfdOSVersionIDLabelKey,
		nfdKernelLabelKey,
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok1 := e.ObjectOld.(*corev1.Node)
			newNode, ok2 := e.ObjectNew.(*corev1.Node)
			if !ok1 || !ok2 {
				return false
			}

			for _, key := range interested {
				oldVal := oldNode.GetLabels()[key]
				newVal := newNode.GetLabels()[key]
				if oldVal != newVal {
					return true
				}
			}
			return false
		},
	}
}
