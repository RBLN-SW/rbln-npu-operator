package components

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

type Patcher interface {
	IsEnabled() bool
	Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error
	CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error
	ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error)
	ComponentName() string
	ComponentNamespace() string
}
