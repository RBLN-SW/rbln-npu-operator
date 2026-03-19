package controller

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	minDelayCR = 100 * time.Millisecond
	maxDelayCR = 3 * time.Second
)

// RBLNClusterPolicyReconciler reconciles a RBLNClusterPolicy object
type RBLNClusterPolicyReconciler struct {
	client.Client
	Log              logr.Logger
	Scheme           *runtime.Scheme
	SingletonCRName  string
	ClusterInfo      *clusterinfo.Info
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
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceclaims,verbs=get
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices,verbs=get;list;watch;create;update;patch;delete

func (r *RBLNClusterPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling RBLNClusterPolicy", "name", req.Name)

	instance, err := r.fetchClusterPolicy(ctx, req)
	if err != nil || instance == nil {
		return ctrl.Result{}, err
	}

	ignored, err := r.handleSingletonPolicy(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ignored {
		return ctrl.Result{}, nil
	}

	namespace, err := resolveNamespace(instance)
	if err != nil {
		return ctrl.Result{}, err
	}

	service := clusterpolicy.NewClusterPolicyService(
		r.Client,
		r.Log,
		r.Scheme,
		instance,
		namespace,
		r.ClusterInfo.OpenShiftVersion,
		r.ClusterInfo.ContainerRuntime,
	)

	err = service.ApplyDriverAutoUpgradeAnnotation(ctx)
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}

	nfdInstalled, rblnNodes, err := service.LabelNodes(ctx)
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}

	if !nfdInstalled {
		r.Log.V(consts.LogLevelWarning).Info("WARNING: NodeFeatureDiscovery is not installed, Rebellions NPU cannot be discovered. Requeue after 30 seconds.")
		if stateErr := updateCRState(ctx, r, instance, rblnv1beta1.ClusterNotReady); stateErr != nil {
			r.Log.V(consts.LogLevelDebug).Error(stateErr, "failed to set ClusterPolicy state")
		}
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	if rblnNodes == 0 {
		r.Log.Info("INFO: No Rebellions NPU discovered. Skip further reconciling.")
		if stateErr := updateCRState(ctx, r, instance, rblnv1beta1.ClusterNotReady); stateErr != nil {
			r.Log.V(consts.LogLevelDebug).Error(stateErr, "failed to set ClusterPolicy state")
		}
		return ctrl.Result{}, nil
	}

	if err := service.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch components in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	if err := service.ReconcileStatus(ctx); err != nil {
		r.Log.Error(err, "Failed to reconcile status in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RBLNClusterPolicyReconciler) fetchClusterPolicy(
	ctx context.Context,
	req ctrl.Request,
) (*rblnv1beta1.RBLNClusterPolicy, error) {
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if kapierrors.IsNotFound(err) {
			if r.SingletonCRName == req.Name {
				r.SingletonCRName = ""
				r.Log.V(consts.LogLevelInfo).Info("singleton RBLNClusterPolicy cleared", "name", req.Name)
			}
			return nil, nil
		}

		wrappedErr := fmt.Errorf("failed to get RBLNClusterPolicy object: %w", err)
		r.Log.Error(err, "failed to get RBLNClusterPolicy object")
		return nil, wrappedErr
	}

	return instance, nil
}

func (r *RBLNClusterPolicyReconciler) handleSingletonPolicy(
	ctx context.Context,
	instance *rblnv1beta1.RBLNClusterPolicy,
) (bool, error) {
	if r.SingletonCRName != "" && r.SingletonCRName != instance.Name {
		r.Log.V(consts.LogLevelDebug).Info("Set RBLNClusterPolicy status as ignored")
		if err := updateCRState(ctx, r, instance, rblnv1beta1.ClusterIgnored); err != nil {
			return false, err
		}
		return true, nil
	}

	if r.SingletonCRName == "" {
		r.SingletonCRName = instance.Name
		r.Log.Info("Set singleton RBLNClusterPolicy", "name", instance.Name)
	}

	return false, nil
}

func resolveNamespace(instance *rblnv1beta1.RBLNClusterPolicy) (string, error) {
	if instance.Spec.Namespace != "" {
		return instance.Spec.Namespace, nil
	}
	if namespace := os.Getenv("OPERATOR_NAMESPACE"); namespace != "" {
		return namespace, nil
	}
	return "", fmt.Errorf("namespace is not configured. Set OPERATOR_NAMESPACE env variable or spec.namespace")
}

func updateCRState(
	ctx context.Context,
	r *RBLNClusterPolicyReconciler,
	instance *rblnv1beta1.RBLNClusterPolicy,
	state rblnv1beta1.ClusterState,
) error {
	if instance.Status.State == state {
		return nil
	}
	instance.SetStatus(state)
	if err := r.Client.Status().Update(ctx, instance); err != nil {
		r.Log.Error(err, "Failed to update ClusterPolicy status")
		return err
	}
	return nil
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
