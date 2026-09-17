package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/metrics"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

const (
	plannedRequeueInterval = time.Second * 10
	// statusPendingRequeueInterval retries a policy whose status the policy
	// controller has not decided yet (avoids racing it on fresh CRs).
	statusPendingRequeueInterval = time.Second * 5
	DriverLabelKey               = "app.kubernetes.io/component"
	DriverLabelValue             = "rbln-driver"
)

type UpgradeReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Namespace    string
	StateManager upgrade.ClusterUpgradeStateManager
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;daemonsets;replicasets;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=rebellions.ai,resources=rblnclusterpolicies/status,verbs=get;update;patch
// Pod eviction matches NPU pods that hold a DRA ResourceClaim instead of a
// resource request, so it reads both the claims and the DeviceClasses that
// mark one as an NPU. Declared here too, not only on the policy controller,
// so the dependency survives a change there.
// +kubebuilder:rbac:groups=resource.k8s.io,resources=deviceclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceclaims,verbs=get

func (r *UpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(consts.VDebug).Info("Reconciling driver upgrade", "policy", req.NamespacedName)

	clusterPolicy := &rblnv1beta1.RBLNClusterPolicy{}
	err := r.Get(ctx, req.NamespacedName, clusterPolicy)
	if err != nil {
		logger.Error(err, "Error getting RBLNClusterPolicy object")
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, r.cleanupIfNoPoliciesLeft(ctx)
		}
		return reconcile.Result{}, err
	}

	// An ignored (non-singleton) policy must not touch cluster-wide upgrade
	// state; an undecided or stale status is retried until the policy
	// controller settles the current generation.
	if clusterPolicy.Status.State == consts.RBLNStateIgnored {
		return reconcile.Result{}, nil
	}
	if clusterPolicy.Status.State == "" ||
		clusterPolicy.Status.ObservedGeneration != clusterPolicy.Generation {
		return reconcile.Result{RequeueAfter: statusPendingRequeueInterval}, nil
	}

	if clusterPolicy.Spec.Driver.UpgradePolicy == nil ||
		!clusterPolicy.Spec.Driver.UpgradePolicy.AutoUpgrade {
		logger.Info("Auto-upgrade disabled; cleaning upgrade state and skipping reconciliation")
		metrics.DriverUpgradeNodes.Reset()
		if err := r.clearUpgradeStatus(ctx, clusterPolicy); err != nil {
			logger.Error(err, "Failed to clear driver upgrade status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.removeNodeUpgradeState(ctx)
	}

	driverLabel := map[string]string{DriverLabelKey: DriverLabelValue}

	state, err := r.StateManager.BuildState(ctx, r.Namespace,
		driverLabel)
	if err != nil {
		logger.Error(err, "Failed to build cluster upgrade state")
		return ctrl.Result{}, err
	}

	applyErr := r.StateManager.ApplyState(ctx, state, clusterPolicy.Spec.Driver.UpgradePolicy)
	if applyErr != nil {
		logger.Error(applyErr, "Failed to apply cluster upgrade state")
	}

	// Published even when ApplyState partially failed — an erroring cycle is
	// when visibility matters most.
	if statusErr := r.publishUpgradeStatus(ctx, clusterPolicy,
		upgrade.SummarizeClusterUpgrade(state)); statusErr != nil {
		logger.Error(statusErr, "Failed to publish driver upgrade status")
		applyErr = errors.Join(applyErr, statusErr)
	}
	if applyErr != nil {
		return ctrl.Result{}, applyErr
	}

	return ctrl.Result{RequeueAfter: plannedRequeueInterval}, nil
}

// cleanupIfNoPoliciesLeft resets cluster-wide upgrade state only when no
// RBLNClusterPolicy remains, so deleting an ignored CR cannot wipe the
// state owned by the active one.
func (r *UpgradeReconciler) cleanupIfNoPoliciesLeft(ctx context.Context) error {
	list := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := r.List(ctx, list); err != nil {
		return err
	}
	if len(list.Items) > 0 {
		return nil
	}
	metrics.DriverUpgradeNodes.Reset()
	return r.removeNodeUpgradeState(ctx)
}

// removeNodeUpgradeState tears the workflow's node bookkeeping down: the state
// label, the initial-state annotation and the timeout clocks go, and the cordon
// this rollout took is lifted. Label and cordon move in one patch, so a node can
// never be left uncordoned while still labeled, or labeled while already back
// in service.
//
// The initial-state annotation has to go with the label. It records whether the
// node was unschedulable when the rollout admitted it, and left behind it
// outlives the rollout that meant it: the next one would read this rollout's own
// leftover cordon as the administrator's and refuse to lift it forever. The
// timeout clocks are cleared only when their state completes, so a rollout
// paused inside one would hand the next rollout a stale epoch and an instant
// timeout.
//
// A node parked in upgrade-failed is left alone entirely. Its cordon stays on
// purpose, the driver did not come up, and its label has to stay with it:
// stripped of the label, the next rollout would admit the node as unknown, read
// the leftover cordon as the administrator's, wipe the failure reason on the way
// in and finish without ever uncordoning. With the label it is retried as a
// parked node (ProcessUpgradeFailedNodes), which knows the cordon is its own.
func (r *UpgradeReconciler) removeNodeUpgradeState(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Info("Resetting node upgrade state from all nodes")

	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList, client.HasLabels{upgrade.UpgradeStateLabelKey}); err != nil {
		logger.Error(err, "Failed to get node list to reset upgrade state")
		return err
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		state := node.Labels[upgrade.UpgradeStateLabelKey]
		if state == upgrade.UpgradeStateFailed {
			logger.Info("Leaving upgrade-failed node parked with its label and cordon", "node", node.Name)
			continue
		}
		releaseCordon := upgrade.ShouldReleaseCordonOnTeardown(node)
		if err := r.Patch(ctx, node,
			client.RawPatch(types.MergePatchType, upgradeStateTeardownPatch(releaseCordon))); err != nil {
			logger.Error(err, "Failed to reset upgrade state on node", "node", node.Name)
			return err
		}
		if releaseCordon {
			logger.Info("Returned the node to service, lifting any cordon the rollout took",
				"node", node.Name, "state", state)
		}
	}
	return nil
}

func upgradeStateTeardownPatch(releaseCordon bool) []byte {
	patch := fmt.Appendf(nil, `{"metadata":{"labels":{%q:null},"annotations":{%q:null,%q:null,%q:null}}`,
		upgrade.UpgradeStateLabelKey,
		upgrade.UpgradeInitialStateAnnotationKey,
		upgrade.UpgradeValidationStartTimeAnnotationKey,
		upgrade.UpgradeWaitForPodCompletionStartTimeAnnotationKey)
	if releaseCordon {
		patch = append(patch, `,"spec":{"unschedulable":false}`...)
	}
	return append(patch, '}')
}

// clusterPolicyUpgradePredicate also fires on status transitions (state or
// observed generation): a promoted successor changes only status, and the
// GenerationChangedPredicate alone would never wake this controller.
var clusterPolicyUpgradePredicate = predicate.TypedFuncs[*rblnv1beta1.RBLNClusterPolicy]{
	CreateFunc:  func(event.TypedCreateEvent[*rblnv1beta1.RBLNClusterPolicy]) bool { return true },
	DeleteFunc:  func(event.TypedDeleteEvent[*rblnv1beta1.RBLNClusterPolicy]) bool { return true },
	GenericFunc: func(event.TypedGenericEvent[*rblnv1beta1.RBLNClusterPolicy]) bool { return false },
	UpdateFunc: func(e event.TypedUpdateEvent[*rblnv1beta1.RBLNClusterPolicy]) bool {
		return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
			e.ObjectOld.Status.State != e.ObjectNew.Status.State ||
			e.ObjectOld.Status.ObservedGeneration != e.ObjectNew.Status.ObservedGeneration
	},
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
		clusterPolicyUpgradePredicate),
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
