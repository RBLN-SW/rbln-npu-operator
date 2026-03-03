package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	plannedRequeueInterval   = time.Second * 30
	transientRequeueInterval = time.Second * 10
	DriverLabelKey           = "app.kubernetes.io/component"
	DriverLabelValue         = "rbln-driver"
)

type UpgradeReconciler struct {
	client.Client
	Log          logr.Logger
	Scheme       *runtime.Scheme
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

	if clusterPolicy.Spec.SandboxDevicePlugin.IsEnabled() {
		r.Log.Info("SandboxDevicePlugin enabled; skipping advanced driver upgrade and cleaning upgrade state")
		return ctrl.Result{}, r.removeNodeUpgradeStateLabels(ctx)
	}

	if clusterPolicy.Spec.Driver.UpgradePolicy == nil ||
		!clusterPolicy.Spec.Driver.UpgradePolicy.AutoUpgrade {
		r.Log.Info("Auto-upgrade disabled; cleaning upgrade state and skipping reconciliation")
		return ctrl.Result{}, r.removeNodeUpgradeStateLabels(ctx)
	}

	namespace := clusterPolicy.Spec.Namespace
	if namespace == "" {
		namespace = os.Getenv("OPERATOR_NAMESPACE")
	}
	if namespace == "" {
		err = fmt.Errorf("namespace is not configured. Set OPERATOR_NAMESPACE env variable or namespace spec")
		r.Log.Error(err, "Failed to resolve namespace for upgrade state build")
		return ctrl.Result{}, nil
	}

	driverLabel := map[string]string{DriverLabelKey: DriverLabelValue}
	r.Log.Info("Using label selector", "key", DriverLabelKey, "value", DriverLabelValue)

	state, err := r.StateManager.BuildState(ctx, namespace,
		driverLabel)
	if err != nil {
		if errors.Is(err, upgrade.ErrDriverDaemonSetHasUnscheduledPods) {
			r.Log.Info("Driver DaemonSet scheduling is transiently inconsistent; requeueing", "error", err)
			return ctrl.Result{RequeueAfter: transientRequeueInterval}, nil
		}
		r.Log.Error(err, "Failed to build cluster upgrade state")
		return ctrl.Result{}, err
	}

	err = r.StateManager.ApplyState(ctx, namespace, state, clusterPolicy.Spec.Driver.UpgradePolicy)
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
	err := r.List(ctx, nodeList)
	if err != nil {
		r.Log.Error(err, "Failed to get node list to reset upgrade labels")
		return err
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		_, present := node.Labels[upgrade.UpgradeStateLabelKey]
		if present {
			patchBytes := fmt.Appendf(nil, `{"metadata":{"labels":{%q:null}}}`, upgrade.UpgradeStateLabelKey)
			patch := client.RawPatch(types.MergePatchType, patchBytes)
			err = r.Patch(ctx, node, patch)
			if err != nil {
				r.Log.Error(err, "Failed to reset upgrade state label from node", "node", node)
				return err
			}
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
		UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
			label := upgrade.UpgradeStateLabelKey
			return e.ObjectOld.Labels[label] != e.ObjectNew.Labels[label]
		},
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

	dsMapFn := func(ctx context.Context, a *appsv1.DaemonSet) []reconcile.Request {
		ownerRefs := a.GetOwnerReferences()

		ownedByRBLN := false
		for _, owner := range ownerRefs {
			if (owner.APIVersion == rblnv1beta1.GroupVersion.String() && owner.Kind == "RBLNClusterPolicy") ||
				(owner.APIVersion == rblnv1alpha1.GroupVersion.String() && owner.Kind == "RBLNDriver") {
				ownedByRBLN = true
				break
			}
		}

		if !ownedByRBLN {
			return nil
		}

		return getClusterPoliciesToReconcile(ctx, mgr.GetClient())
	}

	appLabelSelector := predicate.NewTypedPredicateFuncs(func(ds *appsv1.DaemonSet) bool {
		ls := metav1.LabelSelector{MatchLabels: map[string]string{DriverLabelKey: DriverLabelValue}}
		selector, _ := metav1.LabelSelectorAsSelector(&ls)
		return selector.Matches(labels.Set(ds.GetLabels()))
	})

	err = c.Watch(
		source.Kind(
			mgr.GetCache(),
			&appsv1.DaemonSet{},
			handler.TypedEnqueueRequestsFromMapFunc[*appsv1.DaemonSet](dsMapFn),
			predicate.And[*appsv1.DaemonSet](
				predicate.TypedGenerationChangedPredicate[*appsv1.DaemonSet]{},
				predicate.Or[*appsv1.DaemonSet](appLabelSelector),
			),
		))
	if err != nil {
		return err
	}

	return nil
}

func getClusterPoliciesToReconcile(ctx context.Context, k8sClient client.Client) []reconcile.Request {
	logger := log.FromContext(ctx)
	opts := []client.ListOption{}
	list := &rblnv1beta1.RBLNClusterPolicyList{}

	err := k8sClient.List(ctx, list, opts...)
	if err != nil {
		logger.Error(err, "Unable to list ClusterPolicies")
		return []reconcile.Request{}
	}

	cpToRec := []reconcile.Request{}

	for _, cp := range list.Items {
		cpToRec = append(cpToRec, reconcile.Request{NamespacedName: types.NamespacedName{
			Name:      cp.GetName(),
			Namespace: cp.GetNamespace(),
		}})
	}

	return cpToRec
}
