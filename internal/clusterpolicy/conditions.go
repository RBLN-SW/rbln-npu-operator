package clusterpolicy

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func (s *ClusterPolicyService) ReconcileStatus(ctx context.Context) error {
	componentsStatus := s.AssembleComponentStatus(ctx)

	instance := &rblnv1beta1.RBLNClusterPolicy{}
	if err := s.client.Get(ctx, client.ObjectKeyFromObject(s.policy), instance); err != nil {
		return fmt.Errorf("get cluster policy for status update: %w", err)
	}

	instance.Status.Components = componentsStatus

	if allComponentsReady(componentsStatus) {
		applyReadyStatus(instance, len(componentsStatus))
	} else {
		applyNotReadyStatus(instance, componentsStatus)
	}

	if err := s.client.Status().Update(ctx, instance); err != nil {
		return fmt.Errorf("update cluster policy status: %w", err)
	}
	return nil
}

func applyReadyStatus(policy *rblnv1beta1.RBLNClusterPolicy, componentCount int) {
	policy.SetStatus(rblnv1beta1.ClusterReady)

	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeComponentsReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AllComponentsReady",
		Message: "All managed components are Ready",
	})

	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeReady,
		Status:  metav1.ConditionTrue,
		Reason:  "AllComponentsReady",
		Message: fmt.Sprintf("All components are Ready (%d/%d)", componentCount, componentCount),
	})
}

func applyNotReadyStatus(
	policy *rblnv1beta1.RBLNClusterPolicy,
	components []rblnv1beta1.RBLNComponentStatus,
) {
	policy.SetStatus(rblnv1beta1.ClusterNotReady)

	notReadyComponents := make([]string, 0, len(components))
	for _, component := range components {
		if component.State == rblnv1beta1.ComponentStateReady {
			continue
		}
		notReadyComponents = append(
			notReadyComponents,
			fmt.Sprintf("%s/%s", component.Namespace, component.Name),
		)
	}

	message := fmt.Sprintf("Components not ready: %s", strings.Join(notReadyComponents, ", "))

	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeComponentsReady,
		Status:  metav1.ConditionFalse,
		Reason:  "SomeComponentsNotReady",
		Message: message,
	})

	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:    consts.RBLNConditionTypeReady,
		Status:  metav1.ConditionFalse,
		Reason:  "SomeComponentsNotReady",
		Message: message,
	})
}

func allComponentsReady(components []rblnv1beta1.RBLNComponentStatus) bool {
	for _, component := range components {
		if component.State != rblnv1beta1.ComponentStateReady {
			return false
		}
	}
	return true
}
