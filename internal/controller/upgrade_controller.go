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
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	rblnv1alpha1 "github.com/RBLN-SW/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/drivermanager"
	"github.com/RBLN-SW/rbln-npu-operator/internal/metrics"
	"github.com/RBLN-SW/rbln-npu-operator/internal/upgrade"
)

const (
	// plannedRequeueInterval paces the workflow while a rollout is in progress.
	plannedRequeueInterval = 10 * time.Second
	// statusPendingRequeueInterval is used while the policy controller has not
	// yet published a status for the policy's current generation; acting on a
	// fresh CR before that would race it.
	statusPendingRequeueInterval = 5 * time.Second

	// DriverLabelKey and DriverLabelValue identify the driver DaemonSets whose
	// rollout the upgrade workflow orchestrates.
	DriverLabelKey   = "app.kubernetes.io/component"
	DriverLabelValue = "rbln-driver"

	upgradeControllerName = "upgrade-controller"
)

// UpgradeReconciler runs one pass of the automatic driver upgrade workflow per
// reconcile: it collects the upgrade state of every node, lets the state
// manager move the nodes through the workflow, and publishes a summary on the
// RBLNClusterPolicy status.
type UpgradeReconciler struct {
	client.Client
	// APIReader bypasses the informer cache. The teardown's verdict comes from
	// the state label and annotations, which the state manager writes through
	// its own direct client; a pass that lists them from the cache can act on
	// the state a node was in before the previous pass moved it.
	APIReader    client.Reader
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

// Reconcile advances the driver upgrade workflow for the policy named in req.
func (r *UpgradeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("policy", req.NamespacedName)
	logger.V(consts.VDebug).Info("Reconciling driver upgrade")

	policy := &rblnv1beta1.RBLNClusterPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.cleanupIfNoPoliciesLeft(ctx)
		}
		logger.Error(err, "Unable to read the RBLNClusterPolicy")
		return ctrl.Result{}, err
	}

	// Only the active singleton may touch cluster-wide upgrade state. A policy
	// the policy controller has marked as ignored is left alone for good; one
	// whose status is missing or stale is revisited once it has been decided.
	switch {
	case policy.Status.State == consts.RBLNStateIgnored:
		return ctrl.Result{}, nil
	case policy.Status.State == "", policy.Status.ObservedGeneration != policy.Generation:
		return ctrl.Result{RequeueAfter: statusPendingRequeueInterval}, nil
	}

	if !autoUpgradeEnabled(policy) {
		return ctrl.Result{}, r.disableWorkflow(ctx, policy)
	}
	return r.advanceRollout(ctx, policy)
}

// autoUpgradeEnabled reports whether the policy asks for the automatic
// upgrade workflow at all.
func autoUpgradeEnabled(policy *rblnv1beta1.RBLNClusterPolicy) bool {
	up := policy.Spec.Driver.UpgradePolicy
	return up != nil && up.AutoUpgrade
}

// driverSelector matches the DaemonSets that roll out the NPU driver.
func driverSelector() map[string]string {
	return map[string]string{DriverLabelKey: DriverLabelValue}
}

// disableWorkflow is the path taken while autoUpgrade is off: the published
// status and every node's workflow bookkeeping are cleared so that a later
// re-enable starts from a clean slate.
func (r *UpgradeReconciler) disableWorkflow(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy) error {
	logger := log.FromContext(ctx)
	logger.Info("Automatic driver upgrade is disabled; clearing upgrade state")
	metrics.DriverUpgradeNodes.Reset()
	if err := r.clearUpgradeStatus(ctx, policy); err != nil {
		logger.Error(err, "Unable to clear the driver upgrade status")
		return err
	}
	return r.removeNodeUpgradeState(ctx)
}

// advanceRollout performs one workflow pass: collect the node states, move
// them forward, publish the summary, and come back after the planned interval.
func (r *UpgradeReconciler) advanceRollout(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy) (ctrl.Result, error) {
	state, err := r.StateManager.BuildState(ctx, r.Namespace, driverSelector())
	if err != nil {
		return ctrl.Result{}, err
	}

	var failures []error
	if err := r.StateManager.ApplyState(ctx, state, policy.Spec.Driver.UpgradePolicy,
		drivermanager.NPUDeviceClass(&policy.Spec), upgrade.ValidationOptions{
			Namespace: r.Namespace, DefaultWorkload: policy.Spec.WorkloadType,
			DRAEnabled: policy.Spec.DRAKubeletPlugin.Enabled, SandboxEnabled: policy.Spec.SandboxDevicePlugin.Enabled,
			PartitionManagerEnabled: policy.Spec.PartitionManager.Enabled,
		}); err != nil {
		failures = append(failures, err)
	}

	// The summary is published even after a failed pass; a pass that errors is
	// exactly when operators need to see where the nodes are.
	if err := r.publishUpgradeStatus(ctx, policy, upgrade.SummarizeClusterUpgrade(state)); err != nil {
		failures = append(failures, err)
	}

	if len(failures) > 0 {
		return ctrl.Result{}, errors.Join(failures...)
	}
	return ctrl.Result{RequeueAfter: plannedRequeueInterval}, nil
}

// cleanupIfNoPoliciesLeft runs after a policy has vanished. Cluster-wide
// upgrade state belongs to whichever policy is active, so it is torn down
// only when no RBLNClusterPolicy is left at all; deleting an ignored
// duplicate must not wipe the state owned by the active one.
func (r *UpgradeReconciler) cleanupIfNoPoliciesLeft(ctx context.Context) error {
	remaining := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := r.List(ctx, remaining); err != nil {
		return err
	}
	if len(remaining.Items) > 0 {
		return nil
	}
	metrics.DriverUpgradeNodes.Reset()
	return r.removeNodeUpgradeState(ctx)
}

// removeNodeUpgradeState tears the workflow's node bookkeeping down: the state
// label, the initial-state annotation, the timeout clocks and the attempt's
// bookkeeping go, and the cordon this rollout took is lifted with the claim
// that marked it. Label, claim and cordon move in one patch, so a node can
// never be left uncordoned while still labeled or claimed, or claimed while
// already back in service (upgrade.TeardownPatch explains each key).
//
// Nodes are found by the state label or by the rollout's cordon claim, not by
// the label alone: the label can be scrubbed by an external tool while the
// rollout is cordoning, and the cordon it left would otherwise be read by
// every later rollout as the administrator's.
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
	if err := r.APIReader.List(ctx, nodeList); err != nil {
		return fmt.Errorf("list nodes for upgrade state teardown: %w", err)
	}

	// Each node is torn down on its own: one rejected patch must not leave the
	// nodes behind it labeled and cordoned until the requeue.
	var errs []error
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !upgrade.CarriesRolloutBookkeeping(node) {
			continue
		}
		state := node.Labels[upgrade.UpgradeStateLabelKey]
		if state == upgrade.UpgradeStateFailed {
			logger.Info("Leaving upgrade-failed node parked with its label and cordon", "node", node.Name)
			continue
		}
		releaseCordon := upgrade.ShouldReleaseCordonOnTeardown(node)
		patch, err := upgrade.TeardownPatch(releaseCordon)
		if err != nil {
			return fmt.Errorf("build teardown patch for node %q: %w", node.Name, err)
		}
		if err = r.Patch(ctx, node, client.RawPatch(types.MergePatchType, patch)); err != nil {
			errs = append(errs, fmt.Errorf("reset upgrade state on node %q: %w", node.Name, err))
			continue
		}
		if releaseCordon {
			logger.Info("Returned the node to service, lifting the cordon the rollout took",
				"node", node.Name, "state", state)
		}
	}
	return errors.Join(errs...)
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

// nodeUpgradeStateChanged wakes the controller only when a node's upgrade
// state label changes. Node creation, deletion and unrelated updates (resource
// usage, heartbeats) are noise for this workflow.
var nodeUpgradeStateChanged = predicate.TypedFuncs[*corev1.Node]{
	CreateFunc:  func(event.TypedCreateEvent[*corev1.Node]) bool { return false },
	DeleteFunc:  func(event.TypedDeleteEvent[*corev1.Node]) bool { return false },
	GenericFunc: func(event.TypedGenericEvent[*corev1.Node]) bool { return false },
	UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
		return e.ObjectOld.Labels[upgrade.UpgradeStateLabelKey] != e.ObjectNew.Labels[upgrade.UpgradeStateLabelKey]
	},
}

// isManagedDriverDaemonSet selects the driver DaemonSets owned by one of the
// operator's custom resources. A third-party DaemonSet that happens to carry
// the driver label is not part of the rollout and is ignored.
func isManagedDriverDaemonSet(ds *appsv1.DaemonSet) bool {
	if ds.GetLabels()[DriverLabelKey] != DriverLabelValue {
		return false
	}
	for _, owner := range ds.GetOwnerReferences() {
		switch {
		case owner.Kind == "RBLNClusterPolicy" && owner.APIVersion == rblnv1beta1.GroupVersion.String(),
			owner.Kind == "RBLNDriver" && owner.APIVersion == rblnv1alpha1.GroupVersion.String():
			return true
		}
	}
	return false
}

// SetupWithManager registers the controller. Besides the policies themselves
// it watches node upgrade-state label changes and generation changes of the
// managed driver DaemonSets; both fan out to every RBLNClusterPolicy, since
// which one is active is decided inside Reconcile.
func (r *UpgradeReconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	informers := mgr.GetCache()
	fanOut := func(ctx context.Context) []reconcile.Request {
		return r.allPolicyRequests(ctx, mgr.GetClient())
	}

	// Without a For() object the builder would log only namespace/name; keep
	// the controller and object keys that dashboards already filter on.
	logFor := func(req *reconcile.Request) logr.Logger {
		l := mgr.GetLogger().WithValues("controller", upgradeControllerName)
		if req != nil {
			l = l.WithValues("object", klog.KRef(req.Namespace, req.Name),
				"namespace", req.Namespace, "name", req.Name)
		}
		return l
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named(upgradeControllerName).
		WithLogConstructor(logFor).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
				minDelayCR, maxDelayCR),
		}).
		WatchesRawSource(source.Kind(informers, &rblnv1beta1.RBLNClusterPolicy{},
			&handler.TypedEnqueueRequestForObject[*rblnv1beta1.RBLNClusterPolicy]{},
			clusterPolicyUpgradePredicate)).
		WatchesRawSource(source.Kind(informers, &corev1.Node{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, _ *corev1.Node) []reconcile.Request {
				return fanOut(ctx)
			}),
			nodeUpgradeStateChanged)).
		WatchesRawSource(source.Kind(informers, &appsv1.DaemonSet{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, _ *appsv1.DaemonSet) []reconcile.Request {
				return fanOut(ctx)
			}),
			predicate.TypedGenerationChangedPredicate[*appsv1.DaemonSet]{},
			predicate.NewTypedPredicateFuncs(isManagedDriverDaemonSet))).
		Complete(r)
}

// allPolicyRequests enqueues every RBLNClusterPolicy. A failed list is logged
// and yields no requests; the next watch event retries.
func (r *UpgradeReconciler) allPolicyRequests(ctx context.Context, c client.Client) []reconcile.Request {
	policies := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := c.List(ctx, policies); err != nil {
		log.FromContext(ctx).Error(err, "Unable to list RBLNClusterPolicies for upgrade reconciliation")
		return nil
	}
	requests := make([]reconcile.Request, len(policies.Items))
	for i := range policies.Items {
		requests[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&policies.Items[i])}
	}
	return requests
}
