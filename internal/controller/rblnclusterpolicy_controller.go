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
)

type RBLNClusterPolicyReconciler struct {
	client.Client
	Log             logr.Logger
	Scheme          *runtime.Scheme
	SingletonCRName string
	ClusterInfo     *clusterinfo.Info
	Conditions      *conditions.Updater
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
		return ctrl.Result{}, nil
	}

	if err := instance.Spec.Validate(); err != nil {
		r.Log.Error(err, "RBLNClusterPolicy spec is invalid")
		if statusErr := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonInvalidSpec, err.Error()); statusErr != nil {
			r.Log.V(consts.LogLevelDebug).Error(statusErr, "failed to set ClusterPolicy status")
		}
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
		if err := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonNFDNotFound, "NodeFeatureDiscovery labels not found"); err != nil {
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

	census, err := service.ReconcileNodes(ctx, candidates)
	if err != nil {
		r.Log.Error(err, "")
		return ctrl.Result{}, err
	}

	if census.TotalNPU == 0 {
		r.Log.Info("No Rebellions NPU discovered; skipping reconcile")
		if err := r.Conditions.SetPolicyNotReady(ctx, instance, consts.RBLNConditionReasonNoRBLNNodes, "No Rebellions NPU discovered"); err != nil {
			r.Log.V(consts.LogLevelDebug).Error(err, "failed to set ClusterPolicy status")
		}
		return ctrl.Result{}, nil
	}

	if err := service.PatchComponents(ctx); err != nil {
		r.Log.Error(err, "Failed to patch components in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	componentStatuses, workloadStatuses := service.AssembleStatus(ctx, census)
	allReady, err := r.reconcileStatus(ctx, instance, namespace, componentStatuses, workloadStatuses)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile status in RBLNClusterPolicy Scope")
		return ctrl.Result{}, err
	}

	if !allReady {
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
		if err := r.Conditions.SetPolicyIgnored(ctx, instance, "Another RBLNClusterPolicy is already active; this policy is ignored"); err != nil {
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

func (r *RBLNClusterPolicyReconciler) reconcileStatus(
	ctx context.Context,
	policy *rblnv1beta1.RBLNClusterPolicy,
	namespace string,
	componentStatuses []rblnv1beta1.RBLNComponentStatus,
	workloadStatuses []rblnv1beta1.RBLNWorkloadStatus,
) (bool, error) {
	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(policy), instance); err != nil {
		return false, fmt.Errorf("get cluster policy for status update: %w", err)
	}

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
		return false, fmt.Errorf("update cluster policy status: %w", err)
	}
	return state == consts.RBLNStateReady, nil
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
			consts.RBLNConditionReasonAllWorkloadsReady,
			"All workloads are ready"
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
