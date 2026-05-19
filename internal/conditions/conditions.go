// Package conditions centralizes Ready-condition + State updates for
// RBLNClusterPolicy and RBLNDriver CRs so that controllers do not duplicate
// the boilerplate of patching .status.state, .status.conditions and
// .status.observedGeneration together.
package conditions

import (
	"context"
	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// Updater uses type-specific methods instead of reflection so call sites
// stay short and type-checked.
type Updater struct {
	Client client.Client
}

func NewUpdater(c client.Client) *Updater {
	return &Updater{Client: c}
}

// ----------------------------- RBLNClusterPolicy ----------------------------

func (u *Updater) SetPolicyReady(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy, reason, message string) error {
	return u.patchPolicy(ctx, policy, consts.RBLNStateReady, metav1.ConditionTrue, reason, message)
}

func (u *Updater) SetPolicyNotReady(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy, reason, message string) error {
	return u.patchPolicy(ctx, policy, consts.RBLNStateNotReady, metav1.ConditionFalse, reason, message)
}

// SetPolicyIgnored signals that another singleton already owns reconciliation.
func (u *Updater) SetPolicyIgnored(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy, message string) error {
	return u.patchPolicy(ctx, policy, consts.RBLNStateIgnored, metav1.ConditionFalse, consts.RBLNConditionReasonPolicyIgnored, message)
}

func (u *Updater) patchPolicy(
	ctx context.Context,
	policy *rblnv1beta1.RBLNClusterPolicy,
	state string,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	policy.Status.State = state
	policy.Status.ObservedGeneration = policy.Generation
	apimeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             status,
		ObservedGeneration: policy.Generation,
		Reason:             reason,
		Message:            message,
	})
	if err := u.Client.Status().Update(ctx, policy); err != nil {
		return fmt.Errorf("update RBLNClusterPolicy status: %w", err)
	}
	return nil
}

// ------------------------------- RBLNDriver --------------------------------

// DriverSummary is passed empty on early-exit paths that fire before the
// service has run; applyDriverSummary preserves prior status fields then.
type DriverSummary struct {
	Namespace    string
	NodePools    []rblnv1alpha1.RBLNDriverPoolStatus
	DesiredNodes int32
	ReadyNodes   int32
}

func (u *Updater) SetDriverReady(ctx context.Context, driver *rblnv1alpha1.RBLNDriver, summary DriverSummary, reason, message string) error {
	driver.Status.State = rblnv1alpha1.DriverStateReady
	applyDriverSummary(driver, summary)
	apimeta.SetStatusCondition(&driver.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: driver.Generation,
		Reason:             reason,
		Message:            message,
	})
	apimeta.SetStatusCondition(&driver.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeError,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: driver.Generation,
		Reason:             reason,
	})
	if err := u.Client.Status().Update(ctx, driver); err != nil {
		return fmt.Errorf("update RBLNDriver status: %w", err)
	}
	return nil
}

func (u *Updater) SetDriverNotReady(ctx context.Context, driver *rblnv1alpha1.RBLNDriver, summary DriverSummary, reason, message string) error {
	driver.Status.State = rblnv1alpha1.DriverStateNotReady
	applyDriverSummary(driver, summary)
	apimeta.SetStatusCondition(&driver.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: driver.Generation,
		Reason:             reason,
		Message:            message,
	})
	if err := u.Client.Status().Update(ctx, driver); err != nil {
		return fmt.Errorf("update RBLNDriver status: %w", err)
	}
	return nil
}

// applyDriverSummary leaves zero-valued fields intact so an early-exit
// reconcile path does not wipe out pool data observed in a previous pass.
func applyDriverSummary(driver *rblnv1alpha1.RBLNDriver, s DriverSummary) {
	if s.Namespace != "" {
		driver.Status.Namespace = s.Namespace
	}
	if s.NodePools != nil {
		driver.Status.NodePools = s.NodePools
		driver.Status.DesiredNodes = s.DesiredNodes
		driver.Status.ReadyNodes = s.ReadyNodes
	}
}

// SetDriverError signals a transient reconcile failure (distinct from a
// configured-degraded state): both the Ready and Error conditions flip.
func (u *Updater) SetDriverError(ctx context.Context, driver *rblnv1alpha1.RBLNDriver, err error) error {
	driver.Status.State = rblnv1alpha1.DriverStateNotReady
	apimeta.SetStatusCondition(&driver.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: driver.Generation,
		Reason:             consts.RBLNConditionReasonError,
	})
	apimeta.SetStatusCondition(&driver.Status.Conditions, metav1.Condition{
		Type:               consts.RBLNConditionTypeError,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: driver.Generation,
		Reason:             consts.RBLNConditionReasonReconcileFailed,
		Message:            err.Error(),
	})
	if statusErr := u.Client.Status().Update(ctx, driver); statusErr != nil {
		return fmt.Errorf("update RBLNDriver status: %w", statusErr)
	}
	return nil
}
