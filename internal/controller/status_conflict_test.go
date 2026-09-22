package controller

import (
	"context"
	"errors"
	"testing"

	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// The upgrade controller publishes its own block and conditions into the same
// RBLNClusterPolicy status, so the policy controller's status Update loses
// the resourceVersion race whenever a rollout is reporting. The conflict has
// to be retried from a fresh read: a merge patch would replace the shared
// conditions list wholesale and drop what the other controller just wrote.
func TestReconcileStatusRetriesConflictAndKeepsForeignConditions(t *testing.T) {
	const upgradeCondition = "DriverUpgradeProgressing"
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := rblnv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	policy := &rblnv1beta1.RBLNClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "policy"}}

	conflicts := 0
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(policy).
		WithStatusSubresource(&rblnv1beta1.RBLNClusterPolicy{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if conflicts > 0 {
					return c.SubResource(sub).Update(ctx, obj, opts...)
				}
				conflicts++
				// The upgrade controller lands its condition first.
				current := &rblnv1beta1.RBLNClusterPolicy{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
					return err
				}
				apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
					Type: upgradeCondition, Status: metav1.ConditionTrue, Reason: "Rolling", Message: "1/3",
				})
				if err := c.Status().Update(ctx, current); err != nil {
					return err
				}
				return kapierrors.NewConflict(schema.GroupResource{Group: "rebellions.ai", Resource: "rblnclusterpolicies"},
					obj.GetName(), errors.New("the object has been modified"))
			},
		}).
		Build()

	reconciler := &RBLNClusterPolicyReconciler{Client: k8sClient, APIReader: k8sClient, Log: logf.Log}
	workloads := []rblnv1beta1.RBLNWorkloadStatus{{Type: "container", State: rblnv1beta1.WorkloadStateReady}}

	allReady, _, err := reconciler.reconcileStatus(ctx, policy, "rbln-system", nil, workloads)
	if err != nil {
		t.Fatalf("reconcileStatus: %v", err)
	}
	if !allReady {
		t.Fatal("allReady = false, want true")
	}
	if conflicts != 1 {
		t.Fatalf("conflicts = %d, want 1", conflicts)
	}

	updated := &rblnv1beta1.RBLNClusterPolicy{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if updated.Status.State != consts.RBLNStateReady {
		t.Errorf("state = %q, want %q", updated.Status.State, consts.RBLNStateReady)
	}
	if apimeta.FindStatusCondition(updated.Status.Conditions, consts.RBLNConditionTypeReady) == nil {
		t.Error("Ready condition missing")
	}
	if apimeta.FindStatusCondition(updated.Status.Conditions, upgradeCondition) == nil {
		t.Errorf("%s condition written concurrently was dropped", upgradeCondition)
	}
}
