package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	rblnv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

const (
	plannedRequeueInterval   = time.Second * 10
	transientRequeueInterval = time.Second * 10
	DriverLabelKey           = "app.kubernetes.io/component"
	DriverLabelValue         = "rbln-driver"
)

type UpgradeReconciler struct {
	client.Client
	Log          logr.Logger
	Scheme       *runtime.Scheme
	Namespace    string
	StateManager upgrade.ClusterUpgradeStateManager
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=apps,resources=deployments;daemonsets;replicasets;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments/finalizers,verbs=update

func (r *UpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling driver upgrade", "name", req.NamespacedName)

	clusterPolicy := &rblnv1beta1.RBLNClusterPolicy{}
	err := r.Get(ctx, req.NamespacedName, clusterPolicy)
	if err != nil {
		r.Log.Error(err, "error getting RBLNClusterPolicy object")
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if clusterPolicy.Spec.Driver.UpgradePolicy == nil ||
		!clusterPolicy.Spec.Driver.UpgradePolicy.AutoUpgrade {
		r.Log.Info("Auto-upgrade disabled; cleaning upgrade state and skipping reconciliation")
		return ctrl.Result{}, r.removeNodeUpgradeStateLabels(ctx)
	}

	driverLabel := map[string]string{DriverLabelKey: DriverLabelValue}

	state, err := r.StateManager.BuildState(ctx, r.Namespace,
		driverLabel)
	if err != nil {
		if errors.Is(err, upgrade.ErrDriverDaemonSetHasUnscheduledPods) {
			r.Log.Info("Driver DaemonSet scheduling is transiently inconsistent; requeueing", "error", err)
			return ctrl.Result{RequeueAfter: transientRequeueInterval}, nil
		}
		r.Log.Error(err, "Failed to build cluster upgrade state")
		return ctrl.Result{}, err
	}

	err = r.StateManager.ApplyState(ctx, r.Namespace, state, clusterPolicy.Spec.Driver.UpgradePolicy)
	if err != nil {
		r.Log.Error(err, "Failed to apply cluster upgrade state")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: plannedRequeueInterval}, nil
}

// removeNodeUpgradeStateLabels loops over nodes in the cluster and removes "rebellions.ai/npu-driver-upgrade-state"
// It is used for cleanup when autoUpgrade feature gets disabled
func (r *UpgradeReconciler) removeNodeUpgradeStateLabels(ctx context.Context) error {
	r.Log.Info("Resetting node upgrade labels from all nodes")

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList, client.HasLabels{upgrade.UpgradeStateLabelKey}); err != nil {
		r.Log.Error(err, "Failed to get node list to reset upgrade labels")
		return err
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		patchBytes := fmt.Appendf(nil, `{"metadata":{"labels":{%q:null}}}`, upgrade.UpgradeStateLabelKey)
		if err := r.Patch(ctx, node, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
			r.Log.Error(err, "Failed to reset upgrade state label from node", "node", node.Name)
			return err
		}
	}
	return nil
}

//nolint:dupl
func (r *UpgradeReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	c, err := controller.New("upgrade-controller", mgr, controller.Options{
		Reconciler: r, MaxConcurrentReconciles: 1,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](minDelayCR, maxDelayCR),
	})
	if err != nil {
		return err
	}

	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&rblnv1beta1.RBLNClusterPolicy{},
		&handler.TypedEnqueueRequestForObject[*rblnv1beta1.RBLNClusterPolicy]{},
		predicate.TypedGenerationChangedPredicate[*rblnv1beta1.RBLNClusterPolicy]{}),
	)
	if err != nil {
		return err
	}

	nodeMapFn := func(ctx context.Context, o *corev1.Node) []reconcile.Request {
		return getClusterPoliciesToReconcile(ctx, mgr.GetClient())
	}

	upgradeStateLabelPredicate := predicate.TypedFuncs[*corev1.Node]{
		CreateFunc: func(event.TypedCreateEvent[*corev1.Node]) bool { return false },
		UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
			label := upgrade.UpgradeStateLabelKey
			return e.ObjectOld.Labels[label] != e.ObjectNew.Labels[label]
		},
		DeleteFunc:  func(event.TypedDeleteEvent[*corev1.Node]) bool { return false },
		GenericFunc: func(event.TypedGenericEvent[*corev1.Node]) bool { return false },
	}

	err = c.Watch(
		source.Kind(
			mgr.GetCache(),
			&corev1.Node{},
			handler.TypedEnqueueRequestsFromMapFunc[*corev1.Node](nodeMapFn),
			upgradeStateLabelPredicate,
		),
	)
	if err != nil {
		return err
	}

	dsMapFn := func(ctx context.Context, _ *appsv1.DaemonSet) []reconcile.Request {
		return getClusterPoliciesToReconcile(ctx, mgr.GetClient())
	}

	rblnDriverDSPredicate := predicate.NewTypedPredicateFuncs(func(ds *appsv1.DaemonSet) bool {
		if ds.GetLabels()[DriverLabelKey] != DriverLabelValue {
			return false
		}
		for _, owner := range ds.GetOwnerReferences() {
			if (owner.APIVersion == rblnv1beta1.GroupVersion.String() && owner.Kind == "RBLNClusterPolicy") ||
				(owner.APIVersion == rblnv1alpha1.GroupVersion.String() && owner.Kind == "RBLNDriver") {
				return true
			}
		}
		return false
	})

	err = c.Watch(
		source.Kind(
			mgr.GetCache(),
			&appsv1.DaemonSet{},
			handler.TypedEnqueueRequestsFromMapFunc[*appsv1.DaemonSet](dsMapFn),
			predicate.And[*appsv1.DaemonSet](
				predicate.TypedGenerationChangedPredicate[*appsv1.DaemonSet]{},
				rblnDriverDSPredicate,
			),
		))
	if err != nil {
		return err
	}

	return nil
}

func getClusterPoliciesToReconcile(ctx context.Context, k8sClient client.Client) []reconcile.Request {
	logger := log.FromContext(ctx)
	list := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := k8sClient.List(ctx, list); err != nil {
		logger.Error(err, "Unable to list ClusterPolicies")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, cp := range list.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cp.GetName(),
				Namespace: cp.GetNamespace(),
			},
		})
	}
	return requests
}
