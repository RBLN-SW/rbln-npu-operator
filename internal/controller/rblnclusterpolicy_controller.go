package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	minDelayCR       = 100 * time.Millisecond
	maxDelayCR       = 3 * time.Second
	nfdCheckInterval = 30 * time.Second
)

type RBLNClusterPolicyReconciler struct {
	client.Client
	Log             logr.Logger
	Scheme          *runtime.Scheme
	SingletonCRName string
	ClusterInfo     *clusterinfo.Info
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

	if !instance.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, instance)
	}

	if !controllerutil.ContainsFinalizer(instance, consts.ClusterPolicyFinalizer) {
		controllerutil.AddFinalizer(instance, consts.ClusterPolicyFinalizer)
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
	}

	ignored, err := r.handleSingletonPolicy(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ignored {
		return ctrl.Result{}, nil
	}

	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		return ctrl.Result{}, fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}

	candidates, nfdInstalled, err := clusterpolicy.ListAndClassifyNodes(ctx, r.Client)
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}

	if !nfdInstalled {
		r.Log.V(consts.LogLevelWarning).Info("NodeFeatureDiscovery labels not found", "requeue_after", nfdCheckInterval)
		if err := r.setNotReadyStatus(ctx, instance, consts.RBLNConditionReasonNFDNotFound, "NodeFeatureDiscovery labels not found"); err != nil {
			r.Log.V(consts.LogLevelDebug).Error(err, "failed to set ClusterPolicy status")
		}
		return ctrl.Result{RequeueAfter: nfdCheckInterval}, nil
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

	rblnNodes, err := service.ReconcileNodes(ctx, candidates)
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}

	if rblnNodes == 0 {
		r.Log.Info("No Rebellions NPU discovered; skipping reconcile")
		if err := r.setNotReadyStatus(ctx, instance, consts.RBLNConditionReasonNoRBLNNodes, "No Rebellions NPU discovered"); err != nil {
			r.Log.V(consts.LogLevelDebug).Error(err, "failed to set ClusterPolicy status")
		}
		return ctrl.Result{}, nil
	}

	if err := service.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch components in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	componentStatuses := service.AssembleComponentStatus(ctx)
	if err := r.reconcileStatus(ctx, instance, componentStatuses); err != nil {
		r.Log.Error(err, "Failed to reconcile status in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RBLNClusterPolicyReconciler) handleDeletion(
	ctx context.Context,
	instance *rblnv1beta1.RBLNClusterPolicy,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance, consts.ClusterPolicyFinalizer) {
		return ctrl.Result{}, nil
	}

	r.Log.Info("Cleaning up all components before RBLNClusterPolicy deletion", "name", instance.Name)

	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		return ctrl.Result{}, fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}

	openshiftVersion := ""
	containerRuntime := ""
	if r.ClusterInfo != nil {
		openshiftVersion = r.ClusterInfo.OpenShiftVersion
		containerRuntime = r.ClusterInfo.ContainerRuntime
	}

	service := clusterpolicy.NewClusterPolicyService(
		r.Client, r.Log, r.Scheme, instance, namespace,
		openshiftVersion, containerRuntime,
	)
	if err := service.CleanUpAllComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to clean up components during deletion")
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(instance, consts.ClusterPolicyFinalizer)
	if err := r.Update(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}

	r.SingletonCRName = ""
	r.Log.Info("Successfully cleaned up RBLNClusterPolicy", "name", instance.Name)
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
		if err := r.setNotReadyStatus(ctx, instance, consts.RBLNConditionReasonPolicyIgnored, "Another RBLNClusterPolicy is already active; this policy is ignored"); err != nil {
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

func (r *RBLNClusterPolicyReconciler) setNotReadyStatus(
	ctx context.Context,
	instance *rblnv1beta1.RBLNClusterPolicy,
	reason, message string,
) error {
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: instance.Generation,
		Reason:             reason,
		Message:            message,
	})
	instance.Status.ObservedGeneration = instance.Generation
	if err := r.Client.Status().Update(ctx, instance); err != nil {
		r.Log.Error(err, "Failed to update ClusterPolicy status")
		return err
	}
	return nil
}

func (r *RBLNClusterPolicyReconciler) reconcileStatus(
	ctx context.Context,
	policy *rblnv1beta1.RBLNClusterPolicy,
	componentStatuses []rblnv1beta1.RBLNComponentStatus,
) error {
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(policy), instance); err != nil {
		return fmt.Errorf("get cluster policy for status update: %w", err)
	}

	instance.Status.Components = componentStatuses
	instance.Status.ObservedGeneration = instance.Generation

	allReady := true
	for _, c := range componentStatuses {
		if c.State != rblnv1beta1.ComponentStateReady {
			allReady = false
			break
		}
	}

	if allReady {
		n := len(componentStatuses)
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               consts.RBLNConditionTypeComponentsReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: instance.Generation,
			Reason:             consts.RBLNConditionReasonAllComponentsReady,
			Message:            "All managed components are Ready",
		})
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               consts.RBLNConditionTypeReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: instance.Generation,
			Reason:             consts.RBLNConditionReasonAllComponentsReady,
			Message:            fmt.Sprintf("All components are Ready (%d/%d)", n, n),
		})
	} else {
		notReady := make([]string, 0, len(componentStatuses))
		for _, c := range componentStatuses {
			if c.State != rblnv1beta1.ComponentStateReady {
				notReady = append(notReady, fmt.Sprintf("%s/%s", c.Namespace, c.Name))
			}
		}
		message := fmt.Sprintf("Components not ready: %s", strings.Join(notReady, ", "))
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               consts.RBLNConditionTypeComponentsReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: instance.Generation,
			Reason:             consts.RBLNConditionReasonSomeNotReady,
			Message:            message,
		})
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               consts.RBLNConditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: instance.Generation,
			Reason:             consts.RBLNConditionReasonSomeNotReady,
			Message:            message,
		})
	}

	if err := r.Client.Status().Update(ctx, instance); err != nil {
		return fmt.Errorf("update cluster policy status: %w", err)
	}
	return nil
}

func (r *RBLNClusterPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	genChanged := predicate.GenerationChangedPredicate{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&rblnv1beta1.RBLNClusterPolicy{}).
		Owns(&appsv1.DaemonSet{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.ConfigMap{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.Service{}, builder.WithPredicates(genChanged)).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.singletonRequest),
			builder.WithPredicates(predicate.Or(
				k8sutil.NodeLabelPrefixChangedPredicate("rebellions.ai/"),
				k8sutil.NodeLabelKeyPredicate(
					consts.NFDDevicePCILabelKey,
					consts.NFDDevicePCIAltLabelKey,
				),
			)),
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
