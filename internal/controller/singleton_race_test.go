package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func newSingletonTestReconciler(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *RBLNClusterPolicyReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := rblnv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return &RBLNClusterPolicyReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(funcs).WithObjects(objs...).Build(),
		Log:    logf.Log,
	}
}

// The mapper must not clear ownership that has already moved to another
// policy between the delete event and the callback.
func TestClearSingletonIfDoesNotClobberNewOwner(t *testing.T) {
	r := newSingletonTestReconciler(t, interceptor.Funcs{})
	r.SingletonCRName = "successor"

	if cleared := r.clearSingletonIf("old-owner"); cleared {
		t.Fatal("clearSingletonIf must not clear a different owner")
	}
	if got := r.singletonOwner(); got != "successor" {
		t.Fatalf("owner = %q, want successor", got)
	}
}

func TestPolicyDeletedListFailureReturnsNoRequests(t *testing.T) {
	r := newSingletonTestReconciler(t, interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return fmt.Errorf("list rejected by test")
		},
	})
	r.SingletonCRName = "gone"

	reqs := r.policyDeleted(context.Background(),
		&rblnv1beta1.RBLNClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "gone"}})
	if len(reqs) != 0 {
		t.Fatalf("expected no requests on list failure, got %d", len(reqs))
	}
	// Ownership is still released so the periodic ignored-recheck fallback
	// can promote a survivor.
	if got := r.singletonOwner(); got != "" {
		t.Fatalf("owner = %q, want cleared", got)
	}
}

// Exercises mapper vs reconcile-worker style concurrent access; meaningful
// under `go test -race`.
func TestSingletonAccessorsConcurrency(t *testing.T) {
	r := newSingletonTestReconciler(t, interceptor.Funcs{},
		&rblnv1beta1.RBLNClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "p1"}})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("p%d", n%2+1)
			r.claimSingletonIfVacant(name)
			_ = r.singletonOwner()
			r.clearSingletonIf(name)
			_ = r.policyDeleted(context.Background(),
				&rblnv1beta1.RBLNClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: name}})
		}(i)
	}
	wg.Wait()
}
