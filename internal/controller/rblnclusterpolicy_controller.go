package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
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
	"github.com/rebellions-sw/rbln-npu-operator/internal/metrics"
)

const (
	minDelayCR       = 100 * time.Millisecond
	maxDelayCR       = 3 * time.Second
	nfdCheckInterval = 30 * time.Second
	// singletonRecheckInterval re-evaluates ignored policies so a survivor
	// is promoted even if the deletion mapper missed.
	singletonRecheckInterval = 30 * time.Second
)

type RBLNClusterPolicyReconciler struct {
	client.Client
	Log             logr.Logger
	Scheme          *runtime.Scheme
	SingletonCRName string
	ClusterInfo     *clusterinfo.Info
	Conditions      *conditions.Updater
	Recorder        record.EventRecorder
	// singletonMu guards SingletonCRName: the deletion mapper runs on the
	// informer goroutine, concurrently with the reconcile worker.
	singletonMu sync.Mutex
}

func (r *RBLNClusterPolicyReconciler) singletonOwner() string {
	r.singletonMu.Lock()
	defer r.singletonMu.Unlock()
	return r.SingletonCRName
}

// claimSingletonIfVacant makes the named policy the singleton when none is
// set; returns the resulting owner.
func (r *RBLNClusterPolicyReconciler) claimSingletonIfVacant(name string) string {
	r.singletonMu.Lock()
	defer r.singletonMu.Unlock()
	if r.SingletonCRName == "" {
		r.SingletonCRName = name
	}
	return r.SingletonCRName
}

// clearSingletonIf atomically clears ownership only if it still belongs to
// name, so a freshly promoted policy is never clobbered.
func (r *RBLNClusterPolicyReconciler) clearSingletonIf(name string) bool {
	r.singletonMu.Lock()
	defer r.singletonMu.Unlock()
	if r.SingletonCRName == name {
		r.SingletonCRName = ""
		return true
	}
	return false
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
// +kubebuilder:rbac:groups=nfd.k8s-sigs.io;nfd.openshift.io,resources=nodefeaturerules,verbs=get;list;watch;create;update;patch;delete

func (r *RBLNClusterPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.V(consts.VDebug).Info("Reconciling RBLNClusterPolicy", "policy", req.Name)
	metrics.ReconcileTotal.WithLabelValues("clusterpolicy").Inc()

	instance, err := r.fetchClusterPolicy(ctx, req)
	if err != nil || instance == nil {
		return ctrl.Result{}, err
	}

	ignored, err := r.handleSingletonPolicy(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ignored {
		// Periodic recheck is the promotion fallback when the deletion
		// mapper misses (e.g. its List failed).
		return ctrl.Result{RequeueAfter: singletonRecheckInterval}, nil
	}

	if err := instance.Spec.Validate(); err != nil {
		r.Log.Error(err, "RBLNClusterPolicy spec is invalid")
		if statusErr := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonInvalidSpec, err.Error()); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set ClusterPolicy status")
		}
		return ctrl.Result{}, nil
	}

	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		return ctrl.Result{}, fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}

	candidates, nfdInstalled, err := clusterpolicy.ListAndClassifyNodes(ctx, r.Client)
	if err != nil {
		r.Log.Error(err, "Failed to list and classify NPU candidate nodes")
		return ctrl.Result{}, err
	}

	if !nfdInstalled {
		r.Log.Info("NodeFeatureDiscovery labels not found", "requeueAfter", nfdCheckInterval)
		if err := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonNFDNotFound, "NodeFeatureDiscovery labels not found"); err != nil {
			r.Log.Error(err, "Failed to set ClusterPolicy status")
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

	census, err := service.ReconcileNodes(ctx, candidates)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile node labels and annotations")
		return ctrl.Result{}, err
	}

	if census.TotalNPU == 0 {
		r.Log.Info("No Rebellions NPU discovered; skipping reconcile")
		if err := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonNoRBLNNodes, "No Rebellions NPU discovered"); err != nil {
			r.Log.Error(err, "Failed to set ClusterPolicy status")
		}
		return ctrl.Result{}, nil
	}

	if err := service.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch components in RBLNClusterPolicy Scope")
		recordEvent(r.Recorder, instance, corev1.EventTypeWarning, consts.RBLNEventReasonComponentApplyFailed,
			fmt.Sprintf("Failed to apply component: %v", err))
		// Events are garbage collected, so the failure reason has to reach
		// .status too or it is lost an hour later.
		if statusErr := r.Conditions.SetPolicyNotReady(ctx, instance,
			consts.RBLNEventReasonComponentApplyFailed, err.Error()); statusErr != nil {
			r.Log.Error(statusErr, "Failed to set ClusterPolicy status")
		}
		return ctrl.Result{}, err
	}

	componentStatuses, workloadStatuses := service.AssembleStatus(ctx, census)
	allReady, notReadyMsg, err := r.reconcileStatus(ctx, instance, namespace, componentStatuses, workloadStatuses)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile status in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	if !allReady {
		// Without this the 5s requeue loop is silent at the default gate: the
		// only trace of a stuck policy would be .status, and the per-component
		// detail behind it is VDebug. Mirrors the driver controller's
		// "Driver components not ready".
		r.Log.Info("Cluster policy components not ready", "policy", instance.Name, "reason", notReadyMsg)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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
			if r.clearSingletonIf(req.Name) {
				r.Log.Info("Singleton RBLNClusterPolicy cleared", "policy", req.Name)
			}
			return nil, nil
		}

		wrappedErr := fmt.Errorf("failed to get RBLNClusterPolicy object: %w", err)
		r.Log.Error(err, "Failed to get RBLNClusterPolicy object")
		return nil, wrappedErr
	}

	return instance, nil
}

func (r *RBLNClusterPolicyReconciler) handleSingletonPolicy(
	ctx context.Context,
	instance *rblnv1beta1.RBLNClusterPolicy,
) (bool, error) {
	owner := r.claimSingletonIfVacant(instance.Name)
	if owner != instance.Name {
		r.Log.V(consts.VDebug).Info("Set RBLNClusterPolicy status as ignored")
		wasIgnored := instance.Status.State == consts.RBLNStateIgnored
		if err := r.Conditions.SetPolicyIgnored(ctx, instance, "Another RBLNClusterPolicy is already active; this policy is ignored"); err != nil {
			return false, err
		}
		if !wasIgnored {
			recordEvent(r.Recorder, instance, corev1.EventTypeNormal, consts.RBLNConditionReasonPolicyIgnored,
				"Another RBLNClusterPolicy is already active; this policy is ignored")
		}
		return true, nil
	}

	return false, nil
}

func (r *RBLNClusterPolicyReconciler) reconcileStatus(
	ctx context.Context,
	policy *rblnv1beta1.RBLNClusterPolicy,
	namespace string,
	componentStatuses []rblnv1beta1.RBLNComponentStatus,
	workloadStatuses []rblnv1beta1.RBLNWorkloadStatus,
) (allReady bool, notReadyMsg string, err error) {
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(policy), instance); err != nil {
		return false, "", fmt.Errorf("get cluster policy for status update: %w", err)
	}
	prevState := instance.Status.State

	instance.Status.Namespace = namespace
	instance.Status.Components = componentStatuses
	instance.Status.Workloads = workloadStatuses
	instance.Status.ObservedGeneration = instance.Generation

	state, reason, message := summariseWorkloadStatuses(workloadStatuses)
	instance.Status.State = state
	exportWorkloadMetrics(workloadStatuses, state)

	condStatus := metav1.ConditionTrue
	if state != consts.RBLNStateReady {
		condStatus = metav1.ConditionFalse
	}
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             condStatus,
		ObservedGeneration: instance.Generation,
		Reason:             reason,
		Message:            message,
	})

	if err := r.Client.Status().Update(ctx, instance); err != nil {
		return false, "", fmt.Errorf("update cluster policy status: %w", err)
	}
	// Reusing the condition's reason/message keeps the two from drifting apart.
	if state == consts.RBLNStateReady && prevState != consts.RBLNStateReady {
		recordEvent(r.Recorder, instance, corev1.EventTypeNormal, reason, message)
	}
	// The message doubles as the caller's not-ready log reason, so the log line
	// and the Ready condition cannot report different causes.
	return state == consts.RBLNStateReady, message, nil
}

func exportWorkloadMetrics(workloads []rblnv1beta1.RBLNWorkloadStatus, topState string) {
	for _, w := range workloads {
		metrics.NPUNodes.WithLabelValues(w.Type).Set(float64(w.NodeCount))
		metrics.WorkloadCoverage.WithLabelValues(w.Type).Set(workloadCoverageValue(w.State))
	}
	switch topState {
	case consts.RBLNStateReady:
		metrics.ClusterPolicyReconcileStatus.Set(metrics.ReconcileStatusSuccess)
	default:
		metrics.ClusterPolicyReconcileStatus.Set(metrics.ReconcileStatusNotReady)
		metrics.ReconcileFailed.WithLabelValues("clusterpolicy").Inc()
	}
}

func workloadCoverageValue(s rblnv1beta1.WorkloadState) float64 {
	switch s {
	case rblnv1beta1.WorkloadStateEmpty:
		return metrics.WorkloadCoverageEmpty
	case rblnv1beta1.WorkloadStateReady:
		return metrics.WorkloadCoverageReady
	case rblnv1beta1.WorkloadStateProgressing:
		return metrics.WorkloadCoverageProgressing
	case rblnv1beta1.WorkloadStateUncovered:
		return metrics.WorkloadCoverageUncovered
	default:
		return metrics.WorkloadCoverageEmpty
	}
}

// summariseWorkloadStatuses derives the top-level CR state + Ready-condition
// (reason, message) from per-workload aggregate states. Precedence:
//
//	uncovered  → notReady (most actionable — nodes labeled but no DS)
//	progressing → notReady
//	all ready or empty → ready
func summariseWorkloadStatuses(workloads []rblnv1beta1.RBLNWorkloadStatus) (state, reason, message string) {
	var uncovered, progressing []string
	for _, w := range workloads {
		switch w.State {
		case rblnv1beta1.WorkloadStateUncovered:
			uncovered = append(uncovered, fmt.Sprintf("%s(%d node)", w.Type, w.NodeCount))
		case rblnv1beta1.WorkloadStateProgressing:
			progressing = append(progressing, fmt.Sprintf("%s(%d/%d ready)", w.Type, w.ReadyCount, w.ComponentCount))
		}
	}

	switch {
	case len(uncovered) > 0:
		return consts.RBLNStateNotReady,
			consts.RBLNConditionReasonWorkloadUncovered,
			"Uncovered workload(s): " + strings.Join(uncovered, ", ")
	case len(progressing) > 0:
		return consts.RBLNStateNotReady,
			consts.RBLNConditionReasonWorkloadProgressing,
			"Progressing workload(s): " + strings.Join(progressing, ", ")
	default:
		return consts.RBLNStateReady,
			consts.RBLNConditionReasonAllActiveWorkloadsReady,
			"All workloads with NPU nodes are ready"
	}
}

func (r *RBLNClusterPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	genChanged := predicate.GenerationChangedPredicate{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&rblnv1beta1.RBLNClusterPolicy{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.ConfigMap{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.Service{}, builder.WithPredicates(genChanged)).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.singletonRequest),
			builder.WithPredicates(predicate.Or(
				// The driver controller's owner resolver stamps this label on
				// every reconcile; without the exclusion, each resolver patch
				// would wake this controller right back up for no reason.
				k8sutil.NodeLabelPrefixChangedPredicate("rebellions.ai/", consts.RBLNDriverOwnerLabelKey),
				k8sutil.NodeLabelKeyPredicate(
					consts.NFDDevicePCILabelKey,
					consts.NFDDevicePCIAltLabelKey,
				),
			)),
		).
		Watches(
			&rblnv1beta1.RBLNClusterPolicy{},
			handler.EnqueueRequestsFromMapFunc(r.policyDeleted),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false },
				UpdateFunc:  func(event.UpdateEvent) bool { return false },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				GenericFunc: func(event.GenericEvent) bool { return false },
			}),
		).
		Complete(r)
}

// policyDeleted re-enqueues every remaining policy when one is deleted, so a
// surviving ignored policy is re-evaluated and can be promoted to singleton.
func (r *RBLNClusterPolicyReconciler) policyDeleted(ctx context.Context, o client.Object) []ctrl.Request {
	if r.clearSingletonIf(o.GetName()) {
		r.Log.Info("Singleton RBLNClusterPolicy cleared", "policy", o.GetName())
	}
	list := &rblnv1beta1.RBLNClusterPolicyList{}
	if err := r.List(ctx, list); err != nil {
		// Mapper errors are not retried; the periodic requeue scheduled by
		// ignored-policy reconciles still promotes a survivor eventually.
		r.Log.Error(err, "Failed to list policies after deletion")
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, ctrl.Request{NamespacedName: client.ObjectKey{Name: list.Items[i].Name}})
	}
	return reqs
}

func (r *RBLNClusterPolicyReconciler) singletonRequest(_ context.Context, o client.Object) []ctrl.Request {
	if owner := r.singletonOwner(); owner != "" {
		r.Log.V(consts.VDebug).Info("Rebellions Node label changed, triggering reconcile", "node", o.GetName())
		return []ctrl.Request{
			{
				NamespacedName: client.ObjectKey{
					Name: owner,
				},
			},
		}
	}
	return nil
}
