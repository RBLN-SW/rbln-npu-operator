package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/scope"
)

// RBLNClusterPolicyReconciler reconciles a RBLNClusterPolicy object
type RBLNClusterPolicyReconciler struct {
	client.Client
	Log              logr.Logger
	Scheme           *runtime.Scheme
	SingletonCRName  string
	ClusterInfo      *ClusterInfo
	conditionUpdater conditions.ConditionUpdater
}

// +kubebuilder:rbac:groups=config.openshift.io,resources=clusterversions;proxies,verbs=get;list;watch
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=use,resourceNames=privileged
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts;pods;configmaps;services;nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=resource.k8s.io,resources=deviceclasses;resourceclasses,verbs=get;list;watch;create;update;patch;delete

func (r *RBLNClusterPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling RBLNClusterPolicy", "name", req.Name)

	var err error

	// Fetch RBLNClusterPolicy instance
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err = r.Get(ctx, req.NamespacedName, instance); err != nil {
		// ignore deleted resource
		if kapierrors.IsNotFound(err) {
			// reset singleton if resource is deleted
			if r.SingletonCRName == req.Name {
				r.SingletonCRName = ""
				r.Log.V(consts.LogLevelInfo).Info("singleton RBLNClusterPolicy cleared", "name", req.Name)
			}
			return ctrl.Result{}, nil
		}
		// Error fetching instance. requeue.
		err = fmt.Errorf("failed to get RBLNClusterPolicy object: %v", err)
		r.Log.Error(nil, err.Error())

		return ctrl.Result{}, err
	}

	// RBLNClusterPolicy CR must be unique. Ignore except main CR
	if r.SingletonCRName != "" && r.SingletonCRName != instance.Name {
		r.Log.V(consts.LogLevelDebug).Info("Set RBLNClusterPolicy status as ignored")
		updateCRState(ctx, r, req.NamespacedName, rblnv1beta1.ClusterIgnored)
		return ctrl.Result{}, nil
	}

	if r.SingletonCRName == "" {
		r.SingletonCRName = instance.Name
		r.Log.Info("Set singleton RBLNClusterPolicy", "name", instance.Name)
	}

	// Initialize RBLNClusterPolicyScope
	cpScope, err := scope.NewRBLNClusterPolicyScope(ctx, r.Client, r.Log, r.Scheme, instance, r.ClusterInfo.OpenshiftVersion, r.ClusterInfo.ContainerRuntime)
	if err != nil {
		err = fmt.Errorf("failed to initialize RBLNClusterPolicy Scope: %v", err)
		updateCRState(ctx, r, req.NamespacedName, rblnv1beta1.ClusterNotReady)
		condErr := r.conditionUpdater.SetConditionsError(ctx, instance, conditions.ReconcileFailed, err.Error())
		if condErr != nil {
			r.Log.V(consts.LogLevelDebug).Error(nil, condErr.Error())
		}
		r.Log.Error(nil, err.Error())
		return ctrl.Result{}, err
	}

	nfdInstalled, rblnNodes, err := cpScope.LabelRblnNodes()
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}
	if !nfdInstalled {
		r.Log.V(consts.LogLevelWarning).Info("WARNING: NodeFeatureDiscovery is not installed, Rebellions NPU cannot be discovered. Requeue after 30 seconds.")
		updateCRState(ctx, r, req.NamespacedName, rblnv1beta1.ClusterNotReady)
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// no rebellions nodes in this cluster. skip further reconciling
	if rblnNodes == 0 {
		r.Log.Info("INFO: No Rebellions NPU discovered. Skip further reconciling.")
		updateCRState(ctx, r, req.NamespacedName, rblnv1beta1.ClusterNotReady)
		return ctrl.Result{}, nil
	}

	// patch components
	if err := cpScope.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch components in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	return r.reconcileStatus(ctx, instance, cpScope)
}

func (r *RBLNClusterPolicyReconciler) setClusterReadyStatus(
	rblnPolicy *rblnv1beta1.RBLNClusterPolicy, componentCount int,
) {
	rblnPolicy.SetStatus(rblnv1beta1.ClusterReady)
	meta.SetStatusCondition(&rblnPolicy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeComponentsReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AllComponentsReady",
		Message: "All managed components are Ready",
	})
	meta.SetStatusCondition(&rblnPolicy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AllComponentsReady",
		Message: fmt.Sprintf("All components are Ready (%d/%d)", componentCount, componentCount),
	})
}

func (r *RBLNClusterPolicyReconciler) setClusterNotReadyStatus(
	rblnPolicy *rblnv1beta1.RBLNClusterPolicy,
	components []rblnv1beta1.RBLNComponentStatus,
) {
	rblnPolicy.SetStatus(rblnv1beta1.ClusterNotReady)
	notReadyComponents := make([]string, 0, len(components))
	for _, cs := range components {
		if cs.State == rblnv1beta1.ComponentStateReady {
			continue
		}
		notReadyComponents = append(notReadyComponents, fmt.Sprintf("%s/%s", cs.Namespace, cs.Name))
	}
	message := fmt.Sprintf("Components not ready: %s", strings.Join(notReadyComponents, ", "))
	meta.SetStatusCondition(&rblnPolicy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeComponentsReady,
		Status:  metav1.ConditionFalse,
		Reason:  "SomeComponentsNotReady",
		Message: message,
	})
	meta.SetStatusCondition(&rblnPolicy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeReady,
		Status:  metav1.ConditionFalse,
		Reason:  "SomeComponentsNotReady",
		Message: message,
	})
}

func (r *RBLNClusterPolicyReconciler) reconcileStatus(ctx context.Context, rblnPolicy *rblnv1beta1.RBLNClusterPolicy, cpScope *scope.RBLNClusterPolicyScope) (ctrl.Result, error) {
	componentsStatus := cpScope.AssembleComponentConditions(ctx)
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	err := r.Get(ctx, types.NamespacedName{Name: rblnPolicy.Name}, instance)
	if err != nil {
		r.Log.Error(err, "Failed to get ClusterPolicy instance for status update", "name", rblnPolicy.Name)
		return ctrl.Result{}, err
	}

	instance.Status.Components = componentsStatus

	if allComponentsReady(componentsStatus) {
		r.setClusterReadyStatus(instance, len(componentsStatus))
	} else {
		r.setClusterNotReadyStatus(instance, componentsStatus)
	}

	if err := r.Client.Status().Update(ctx, instance); err != nil {
		r.Log.Error(err, "Failed to update RBLNClusterPolicy status")
		return ctrl.Result{}, fmt.Errorf("failed to update RBLNClusterPolicy status: %w", err)
	}
	return ctrl.Result{}, nil
}

func updateCRState(ctx context.Context, r *RBLNClusterPolicyReconciler, namespacedName types.NamespacedName, state rblnv1beta1.ClusterState) {
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	err := r.Get(ctx, namespacedName, instance)
	if err != nil {
		r.Log.Error(err, "Failed to get ClusterPolicy instance for status update")
	}
	if instance.Status.State == state {
		return
	}
	instance.SetStatus(state)
	err = r.Client.Status().Update(ctx, instance)
	if err != nil {
		r.Log.Error(err, "Failed to update ClusterPolicy status")
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RBLNClusterPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// initialize condition updater
	r.conditionUpdater = conditions.NewClusterPolicyConditionMgr(mgr.GetClient())

	return ctrl.NewControllerManagedBy(mgr).
		For(&rblnv1beta1.RBLNClusterPolicy{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		// Watch Node objects
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.singletonRequest),
			builder.WithPredicates(r.rblnNodeLabelUpdated()),
		).
		Complete(r)
}

func (r *RBLNClusterPolicyReconciler) singletonRequest(_ context.Context, o client.Object) []ctrl.Request {
	if r.SingletonCRName != "" {
		r.Log.V(consts.LogLevelDebug).Info("Rebellions Node label changed, triggering reconcile", "node", o.GetName())
		return []ctrl.Request{
			{
				NamespacedName: client.ObjectKey{
					Name: r.SingletonCRName,
				},
			},
		}
	}
	return nil
}

func (r *RBLNClusterPolicyReconciler) rblnNodeLabelUpdated() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return true },
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok1 := e.ObjectOld.(*corev1.Node)
			newNode, ok2 := e.ObjectNew.(*corev1.Node)
			if !ok1 || !ok2 {
				return false
			}
			// check labels start with rebellions.ai
			oldRblnLabels := k8sutil.FilterMapWithPrefix(oldNode.Labels, "rebellions.ai/")
			newRblnLabels := k8sutil.FilterMapWithPrefix(newNode.Labels, "rebellions.ai/")
			return !reflect.DeepEqual(oldRblnLabels, newRblnLabels)
		},
	}
}

func allComponentsReady(components []rblnv1beta1.RBLNComponentStatus) bool {
	for _, cs := range components {
		if cs.State != rblnv1beta1.ComponentStateReady {
			return false
		}
	}
	return true
}
