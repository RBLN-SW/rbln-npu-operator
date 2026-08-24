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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
	"github.com/rebellions-sw/rbln-npu-operator/internal/metrics"
	"github.com/rebellions-sw/rbln-npu-operator/internal/registry"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

// RBLNDriverReconciler reconciles a RBLNDriver object
type RBLNDriverReconciler struct {
	client.Client
	// APIReader bypasses the informer cache for status reads issued right
	// after a Delete in the same reconcile pass.
	APIReader     client.Reader
	Log           logr.Logger
	Scheme        *runtime.Scheme
	ClusterInfo   *clusterinfo.Info
	Conditions    *conditions.Updater
	Recorder      record.EventRecorder
	ownerResolver *driver.OwnerResolver
	imageChecker  components.ImageChecker
}

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
	r.Log.Info("Reconciling RBLNDriver", "driver", req.Name)
	metrics.ReconcileTotal.WithLabelValues("driver").Inc()

	instance := &rebellionsaiv1alpha1.RBLNDriver{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if kapierrors.IsNotFound(err) {
			// A deleted RBLNDriver must release or reassign its nodes;
			// otherwise stale owner labels pin the old routing forever.
			res, resolveErr := r.ownerResolver.Resolve(ctx)
			if resolveErr != nil {
				return ctrl.Result{}, resolveErr
			}
			// Deletion reassignment is exactly the transition the routing
			// gauges and node events exist to report.
			r.reportResolution(res)
			metrics.CleanupDriverSeries(req.Name)
			return ctrl.Result{}, nil
		}
		wrappedErr := fmt.Errorf("error getting RBLNDriver object: %w", err)
		r.Log.Error(err, "Error getting RBLNDriver object")
		if statusErr := r.Conditions.SetDriverError(ctx, instance, wrappedErr); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, wrappedErr
	}

	// Get the singleton RBLNClusterPolicy object in the cluster.
	clusterPolicyList := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := r.List(ctx, clusterPolicyList); err != nil {
		wrappedErr := fmt.Errorf("error getting RBLNClusterPolicy list: %w", err)
		r.Log.Error(err, "Error getting RBLNClusterPolicy list")
		if statusErr := r.Conditions.SetDriverError(ctx, instance, wrappedErr); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, wrappedErr
	}

	if len(clusterPolicyList.Items) == 0 {
		r.Log.Info("RBLNClusterPolicy not found yet; skipping driver reconcile")
		metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("driver").Inc()
		if statusErr := r.Conditions.SetDriverNotReady(ctx, instance, conditions.DriverSummary{},
			consts.RBLNConditionReasonMissingClusterPolicy, "RBLNClusterPolicy not found in the cluster"); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, nil
	}
	clusterPolicyInstance := *pickActivePolicy(clusterPolicyList.Items)

	// Resolve runs before the validity gate: a CR that becomes invalid must
	// still trigger global reassignment (the resolver excludes invalid CRs
	// internally), so its stale owner labels are cleaned immediately even
	// though this CR's reconcile then short-circuits.
	resolveResult, err := r.ownerResolver.Resolve(ctx)
	if err != nil {
		r.Log.Error(err, "Failed to resolve driver node owners")
		if statusErr := r.Conditions.SetDriverError(ctx, instance, err); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, err
	}
	r.reportResolution(resolveResult)
	conflictNodes := resolveResult.ConflictNodes[instance.Name]

	if err := driver.ValidateDriverSpec(instance); err != nil {
		r.Log.Info("Invalid driver spec; skipping reconcile", "driver", req.Name, "err", err)
		metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("driver").Inc()
		return ctrl.Result{}, r.reportNotReadyOnce(ctx, instance, conditions.DriverSummary{},
			consts.RBLNConditionReasonInvalidSpec, err.Error())
	}

	openshiftVersion := ""
	if r.ClusterInfo != nil {
		openshiftVersion = r.ClusterInfo.OpenShiftVersion
	}
	driverService, err := driver.NewDriverService(
		ctx, r.Client, r.APIReader, r.Log, r.Scheme, instance, &clusterPolicyInstance, r.imageChecker, openshiftVersion,
		resolveResult.OwnedNodes[instance.Name])
	if err != nil {
		r.Log.Error(err, "Failed to initialize RBLNDriver service")
		if statusErr := r.Conditions.SetDriverError(ctx, instance, err); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, err
	}

	if err := driverService.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch driver manager resources")
		recordEvent(r.Recorder, instance, corev1.EventTypeWarning, consts.RBLNEventReasonDriverInstallFailed,
			fmt.Sprintf("Driver installation failed: %v", err))
		if statusErr := r.Conditions.SetDriverError(ctx, instance, err); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, err
	}

	summary, err := r.assembleDriverSummary(ctx, driverService)
	if err != nil {
		r.Log.Error(err, "Failed to assemble driver status")
		if statusErr := r.Conditions.SetDriverError(ctx, instance, err); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set RBLNDriver status")
		}
		return ctrl.Result{}, err
	}

	exportDriverPoolMetrics(instance.Name, summary.NodePools)

	// Diagnostics surface fast-fail states pool status alone cannot: an
	// unlabeled node produces no pool and a missing image produces no
	// DaemonSet, so neither shows up in PoolStatuses. Checked ahead of
	// pool-progress so each gets its own reason.
	diag := driverService.PoolDiagnostics()

	// No watch fires when an image is published, so a missing image needs a
	// poll even when the family-label reason wins below -- otherwise a
	// permanently unlabeled node pins the CR forever and a later publish on a
	// labeled sibling goes unnoticed. NegativeTTL aligns the retry with the
	// checker's negative cache, so each one is a fresh HEAD.
	var res ctrl.Result
	if len(diag.MissingImagePools) > 0 {
		res = ctrl.Result{RequeueAfter: registry.NegativeTTL}
	}
	if len(diag.NodesWithoutFamily) > 0 {
		msg := fmt.Sprintf("%d owned node(s) cannot be mapped to a family-scoped driver image due to a missing or invalid %s label (e.g. %s); verify NFD and the operator-managed NodeFeatureRule are running, or fix a manually-set label value (enable debug logs to see the rejected value)",
			len(diag.NodesWithoutFamily), consts.RBLNNPUFamilyLabelKey, strings.Join(sampleOf(diag.NodesWithoutFamily, 5), ", "))
		if len(diag.MissingImagePools) > 0 {
			msg += "; additionally, " + missingImagePoolsMessage(diag)
		}
		r.Log.Info("Owned nodes lack a usable npu.family label",
			"driver", req.Name, "nodes", len(diag.NodesWithoutFamily), "reason", msg)
		metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("driver").Inc()
		return r.reportNotReadyWithResult(ctx, instance, summary, consts.RBLNConditionReasonFamilyLabelMissing, msg, res)
	}
	if len(diag.MissingImagePools) > 0 {
		msg := missingImagePoolsMessage(diag)
		r.Log.Info("Driver pool image(s) not found in registry",
			"driver", req.Name, "pools", len(diag.MissingImagePools), "reason", msg)
		metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("driver").Inc()
		return r.reportNotReadyWithResult(ctx, instance, summary, consts.RBLNConditionReasonImageNotFound, msg, res)
	}

	if res, ok := r.reportPoolsProgressing(ctx, instance, summary); ok {
		return res, nil
	}

	if res, ok := r.reportSmdProgressing(ctx, instance, summary); ok {
		return res, nil
	}

	// Pool progress takes precedence above; an unresolved selector tie is
	// only reported once this CR's own pools are settled.
	if len(conflictNodes) > 0 {
		msg := fmt.Sprintf(
			"nodeSelector ties with another RBLNDriver on %d node(s) (e.g. %s); tied nodes keep their current owner (check the %s node label) or stay unassigned until selectors are disambiguated",
			len(conflictNodes), strings.Join(sampleOf(conflictNodes, 5), ", "), consts.RBLNDriverOwnerLabelKey)
		r.Log.Info("Unresolved nodeSelector tie with another RBLNDriver",
			"driver", req.Name, "nodes", len(conflictNodes), "reason", msg)
		metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("driver").Inc()
		return ctrl.Result{}, r.reportNotReadyOnce(ctx, instance, summary,
			consts.RBLNConditionReasonConflictingSelector, msg)
	}

	metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusSuccess)
	wasReady := instance.Status.State == consts.RBLNStateReady
	// Ready with zero owned nodes is legal (nothing matches the selector yet)
	// but easy to misread as installed; say so explicitly.
	readyMsg := "All driver pools are ready"
	if len(resolveResult.OwnedNodes[instance.Name]) == 0 {
		readyMsg = "All driver pools are ready (this driver currently owns no nodes)"
	}
	if err := r.Conditions.SetDriverReady(ctx, instance, summary,
		consts.RBLNConditionReasonAllDriverPoolsReady, readyMsg); err != nil {
		return ctrl.Result{}, err
	}
	if !wasReady {
		recordEvent(r.Recorder, instance, corev1.EventTypeNormal, consts.RBLNEventReasonDriverReady,
			fmt.Sprintf("Driver ready on %d/%d nodes", summary.ReadyNodes, summary.DesiredNodes))
	}
	return ctrl.Result{}, nil
}

// assembleDriverSummary gathers pool and smd readiness into the status
// summary; callers read the pool list from summary.NodePools.
func (r *RBLNDriverReconciler) assembleDriverSummary(
	ctx context.Context,
	driverService *driver.DriverService,
) (conditions.DriverSummary, error) {
	pools, desired, ready, err := driverService.AssembleStatus(ctx)
	if err != nil {
		return conditions.DriverSummary{}, fmt.Errorf("assemble driver pool status: %w", err)
	}
	smdStatus, err := driverService.SmdStatus(ctx)
	if err != nil {
		return conditions.DriverSummary{}, fmt.Errorf("assemble rbln-smd status: %w", err)
	}
	return conditions.DriverSummary{
		Namespace:    driverService.Namespace(),
		NodePools:    pools,
		DesiredNodes: desired,
		ReadyNodes:   ready,
		Smd:          smdStatus,
	}, nil
}

// exportDriverPoolMetrics reports ratio=0 for desired==0 pools — alerting
// rules should gate on `desired > 0` to avoid false positives on empty pools.
// The driver's series are dropped first so a pool that vanished (its nodes
// moved away or were relabeled) does not keep exporting its last ratio.
func exportDriverPoolMetrics(driverName string, pools []rebellionsaiv1alpha1.RBLNDriverPoolStatus) {
	metrics.DriverPoolReady.DeletePartialMatch(prometheus.Labels{"driver": driverName})
	for _, p := range pools {
		var ratio float64
		if p.Desired > 0 {
			ratio = float64(p.Ready) / float64(p.Desired)
		}
		metrics.DriverPoolReady.WithLabelValues(driverName, p.Name).Set(ratio)
	}
}

// summarisePoolStates returns "" for both the all-ready and no-pools-yet
// cases; the latter is treated as ready because reconcile may run before
// NFD has labeled any candidate node.
func summarisePoolStates(pools []rebellionsaiv1alpha1.RBLNDriverPoolStatus) string {
	notReady := make([]string, 0, len(pools))
	for _, p := range pools {
		if p.State != rebellionsaiv1alpha1.DriverPoolStateReady {
			notReady = append(notReady, fmt.Sprintf("%s(%d/%d)", p.Name, p.Ready, p.Desired))
		}
	}
	if len(notReady) == 0 {
		return ""
	}
	return "pools progressing: " + strings.Join(notReady, ", ")
}

// sampleOf returns the first n elements of list, or the whole list if it has
// n or fewer -- keeps "e.g. ..." previews in condition messages short
// regardless of how many nodes or pools are affected.
func sampleOf(list []string, n int) []string {
	if len(list) > n {
		return list[:n]
	}
	return list
}

// missingImagePoolsMessage renders the DriverImageNotFound condition message.
// The pool/image list is sampled to 3 and flagged with "e.g." once sampled, so
// the message cannot grow unbounded with fleet size -- matching how the
// family-label and selector-conflict messages flag their own previews.
func missingImagePoolsMessage(diag components.PoolDiagnostics) string {
	pairs := make([]string, 0, len(diag.MissingImagePools))
	for _, p := range diag.MissingImagePools {
		pairs = append(pairs, fmt.Sprintf("%s (%s)", p.Pool, p.Image))
	}
	msg := fmt.Sprintf("%d driver pool(s) are missing their image in the registry%s"+
		"; the check re-runs automatically",
		len(diag.MissingImagePools), listClause(pairs, 3))

	if len(diag.UnreadablePullSecrets) == 0 {
		return msg + "; publish the image(s) to recover"
	}
	// "publish the image" is the wrong advice when the check could not
	// authenticate: on a registry that answers 404 rather than 401/403 to
	// unauthorized reads, a private image that already exists reads as missing.
	// Name the credential problem as the likelier culprit instead of asserting
	// the image is absent.
	return msg + fmt.Sprintf("; the check could not read configured image pull secret(s)%s"+
		" and therefore ran anonymously, so on a registry that answers 404 to unauthorized reads an existing"+
		" private image reads as missing -- fix the secret(s) and the operator's namespace secret-read RBAC first,"+
		" then publish the image(s) if they really are absent (or set DRIVER_IMAGE_CHECK=false to skip this check)",
		listClause(diag.UnreadablePullSecrets, 3))
}

// listClause renders items as ": a, b, c" when the full list is shown, or
// "(e.g. a, b, c)" once sampled down to n -- signaling the list is a sample,
// consistent with how the family-label and selector-conflict messages flag
// their own previews.
func listClause(items []string, n int) string {
	if len(items) <= n {
		return ": " + strings.Join(items, ", ")
	}
	return fmt.Sprintf(" (e.g. %s)", strings.Join(sampleOf(items, n), ", "))
}

// reportPoolsProgressing reports ordinary rollout progress ahead of the Ready
// branch. DriverPoolNotReady is "still catching up", not a new failure, so it
// emits no event. Returns ok=false when nothing is progressing.
func (r *RBLNDriverReconciler) reportPoolsProgressing(
	ctx context.Context,
	instance *rebellionsaiv1alpha1.RBLNDriver,
	summary conditions.DriverSummary,
) (ctrl.Result, bool) {
	msg := summarisePoolStates(summary.NodePools)
	if msg == "" {
		return ctrl.Result{}, false
	}
	r.Log.Info("Driver components not ready", "driver", instance.Name, "reason", msg)
	metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
	metrics.ReconcileFailed.WithLabelValues("driver").Inc()
	if statusErr := r.Conditions.SetDriverNotReady(ctx, instance, summary,
		consts.RBLNConditionReasonDriverPoolNotReady, msg); statusErr != nil {
		r.Log.Error(statusErr, "Failed to set RBLNDriver status")
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true
}

// reportSmdProgressing mirrors reportPoolsProgressing for the per-CR rbln-smd
// DaemonSet: catching up is not a failure, so it emits no event. Runs after
// the pool branch so driver rollout progress always wins the Ready reason.
// Returns ok=false when smd is absent (gated off) or ready.
func (r *RBLNDriverReconciler) reportSmdProgressing(
	ctx context.Context,
	instance *rebellionsaiv1alpha1.RBLNDriver,
	summary conditions.DriverSummary,
) (ctrl.Result, bool) {
	if summary.Smd == nil || summary.Smd.State == rebellionsaiv1alpha1.DriverPoolStateReady {
		return ctrl.Result{}, false
	}
	msg := fmt.Sprintf("rbln-smd DaemonSet is progressing: %d of %d pods are Ready",
		summary.Smd.Ready, summary.Smd.Desired)
	if summary.Smd.Desired == 0 {
		// "0 of 0 pods are Ready" misleads: desired drops to 0 whenever
		// k8s-driver-manager pauses the deploy label during a driver pod
		// start, or before any owned node carries it.
		msg = fmt.Sprintf("rbln-smd DaemonSet is progressing: no eligible nodes (%s=true absent on owned nodes — paused during a driver pod start, or not yet labeled)",
			consts.RBLNDeployRBLNDaemonLabelKey)
	}
	r.Log.Info("Driver components not ready", "driver", instance.Name, "reason", msg)
	metrics.DriverReconcileStatus.WithLabelValues(instance.Name).Set(metrics.ReconcileStatusNotReady)
	metrics.ReconcileFailed.WithLabelValues("driver").Inc()
	if statusErr := r.Conditions.SetDriverNotReady(ctx, instance, summary,
		consts.RBLNConditionReasonSmdNotReady, msg); statusErr != nil {
		r.Log.Error(statusErr, "Failed to set RBLNDriver status")
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true
}

// reportNotReadyOnce marks the CR NotReady and emits the matching Warning
// event once per (reason, generation) -- message-only churn under the same
// pair updates the condition without re-notifying, which is the spam this
// dedup exists to prevent. The condition is read before SetDriverNotReady
// mutates status in place. The status-write error is returned, not logged:
// callers return no-requeue, so a dropped write would strand the condition.
func (r *RBLNDriverReconciler) reportNotReadyOnce(
	ctx context.Context,
	instance *rebellionsaiv1alpha1.RBLNDriver,
	summary conditions.DriverSummary,
	reason, msg string,
) error {
	ready := apimeta.FindStatusCondition(instance.Status.Conditions, consts.RBLNConditionTypeReady)
	first := ready == nil ||
		ready.Reason != reason ||
		ready.ObservedGeneration != instance.Generation
	if statusErr := r.Conditions.SetDriverNotReady(ctx, instance, summary, reason, msg); statusErr != nil {
		return statusErr
	}
	if first {
		recordEvent(r.Recorder, instance, corev1.EventTypeWarning, reason, msg)
	}
	return nil
}

// reportNotReadyWithResult adds a non-zero Result to reportNotReadyOnce. On a
// status-write failure it returns a zero Result with the error, because
// controller-runtime warns about and discards a Result returned alongside an
// error; its rate-limited retry drives the next attempt instead.
func (r *RBLNDriverReconciler) reportNotReadyWithResult(
	ctx context.Context,
	instance *rebellionsaiv1alpha1.RBLNDriver,
	summary conditions.DriverSummary,
	reason, msg string,
	res ctrl.Result,
) (ctrl.Result, error) {
	if err := r.reportNotReadyOnce(ctx, instance, summary, reason, msg); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
}

// reportResolution publishes routing gauges and per-node transition events.
// Gauges reset first: a pass covers every driver, so series from deleted ones
// must not linger. Events fire on transitions only -- the resolver runs every
// reconcile, and repeated per-node Warnings would drain client-go's event
// spam budget for that Node; the gauge carries the steady state.
func (r *RBLNDriverReconciler) reportResolution(res *driver.ResolveResult) {
	metrics.DriverOwnedNodes.Reset()
	metrics.DriverSelectorConflicts.Reset()
	for name, nodes := range res.OwnedNodes {
		metrics.DriverOwnedNodes.WithLabelValues(name).Set(float64(len(nodes)))
	}
	for name, nodes := range res.ConflictNodes {
		metrics.DriverSelectorConflicts.WithLabelValues(name).Set(float64(len(nodes)))
	}
	metrics.DriverUncoveredNodes.Set(float64(len(res.UncoveredNodes)))

	for _, change := range res.OwnerChanges {
		if change.Owner == "" {
			recordEvent(r.Recorder, change.Node, corev1.EventTypeWarning, consts.RBLNEventReasonDriverNodeUncovered,
				"no RBLNDriver owns this NPU node (no selector matches, or an unresolved selector tie); the driver will not be installed here")
			continue
		}
		recordEvent(r.Recorder, change.Node, corev1.EventTypeNormal, consts.RBLNEventReasonDriverOwnerChanged,
			fmt.Sprintf("driver owner set to %q", change.Owner))
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RBLNDriverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.ownerResolver = driver.NewOwnerResolver(mgr.GetClient(), r.Log)

	// Unset means enabled (the common case); only a value that's actually
	// present but fails to parse is worth a Warning -- an unset var must not
	// log on every operator start.
	driverImageCheckEnabled := true
	if raw := os.Getenv("DRIVER_IMAGE_CHECK"); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			r.Log.Info("DRIVER_IMAGE_CHECK is not a valid bool; defaulting to enabled", "value", raw)
		} else {
			driverImageCheckEnabled = parsed
		}
	}

	r.imageChecker = registry.NewChecker(r.Log, registry.WithDisabled(!driverImageCheckEnabled))

	mapFn := func(ctx context.Context, _ client.Object) []reconcile.Request {
		list := &rebellionsaiv1alpha1.RBLNDriverList{}
		if err := mgr.GetClient().List(ctx, list); err != nil {
			r.Log.Error(err, "Unable to list RBLNDriver resources for RBLNClusterPolicy event")
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
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](minDelayCR, maxDelayCR),
		}).
		For(&rebellionsaiv1alpha1.RBLNDriver{}).
		Owns(&appsv1.DaemonSet{}).
		Watches(&rblnv1beta1.RBLNClusterPolicy{}, handler.EnqueueRequestsFromMapFunc(mapFn)).
		// Fan out spec changes only: one CR's selector edit changes every
		// other CR's routing verdict, while status-only updates are noise.
		// DeletionTimestamp flips are fanned out too: a finalizer-bearing
		// delete never bumps generation, yet the resolver excludes deleting
		// CRs, so survivors must re-resolve to pick up the released nodes.
		Watches(&rebellionsaiv1alpha1.RBLNDriver{}, handler.EnqueueRequestsFromMapFunc(mapFn),
			builder.WithPredicates(predicate.Or(
				predicate.GenerationChangedPredicate{},
				k8sutil.DeletionTimestampChangedPredicate(),
			))).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(mapFn),
			// Owner resolution keys off arbitrary user-chosen selector labels,
			// so no fixed key list can safely filter node updates; the
			// resolver's own owner-label writes are excluded so patching it
			// doesn't requeue every driver right back into another resolve.
			builder.WithPredicates(k8sutil.NodeLabelsChangedExceptPredicate(consts.RBLNDriverOwnerLabelKey)),
		).
		Complete(r)
}

// pickActivePolicy prefers the non-ignored singleton, breaking ties by oldest
// creationTimestamp then name. The ClusterPolicy controller's own election is
// first-reconcile-wins on in-memory arrival order, which is not observable
// from here; its only cross-controller trace is Status.State != ignored, so
// this converges on the same policy and can disagree only transiently.
func pickActivePolicy(items []rblnv1beta1.RBLNClusterPolicy) *rblnv1beta1.RBLNClusterPolicy {
	var best *rblnv1beta1.RBLNClusterPolicy
	for i := range items {
		p := &items[i]
		if p.Status.State == consts.RBLNStateIgnored {
			continue
		}
		if best == nil ||
			p.CreationTimestamp.Before(&best.CreationTimestamp) ||
			(p.CreationTimestamp.Equal(&best.CreationTimestamp) && p.Name < best.Name) {
			best = p
		}
	}
	if best == nil {
		return &items[0]
	}
	return best
}
