package controller

import (
	"context"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

var _ = Describe("RBLNDriver startup owner resolution", func() {
	DescribeTable("cleans owners whose deletion event preceded leadership", func(failure string) {
		ctx := context.Background()
		namespace := createTestNamespace(ctx, "driver-startup")
		driver := newDriverFixture(namespace)
		createDriverFixture(ctx, driver)

		// Cover both nodes still in the driver-deploy domain and nodes that
		// left it while the controller was unavailable.
		nodeNames := []string{namespace + "-deploy", namespace + "-outside"}
		for i, name := range nodeNames {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					consts.RBLNDriverOwnerLabelKey:  driver.Name,
					consts.RBLNDeployDriverLabelKey: []string{"true", "false"}[i],
					"test.rebellions.ai/keep":       "unchanged",
				},
			}}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })
		}

		// Hold the lease so deletion happens while the new manager is a
		// follower. No CR or child remains to enqueue a reconcile at startup.
		now := metav1.NowMicro()
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "driver-startup", Namespace: namespace},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To("previous-operator"),
				LeaseDurationSeconds: ptr.To(int32(3600)),
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}
		Expect(k8sClient.Create(ctx, lease)).To(Succeed())

		var passes, successfulPatches atomic.Int32
		var failed, partialBeforeFailure atomic.Bool
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                  k8sClient.Scheme(),
			Metrics:                 metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress:  "0",
			LeaderElection:          true,
			LeaderElectionNamespace: namespace,
			LeaderElectionID:        lease.Name,
			LeaseDuration:           ptr.To(5 * time.Second),
			RenewDeadline:           ptr.To(3 * time.Second),
			RetryPeriod:             ptr.To(100 * time.Millisecond),
			Controller:              config.Controller{SkipNameValidation: ptr.To(true)},
			NewClient: func(cfg *rest.Config, opts client.Options) (client.Client, error) {
				c, err := client.NewWithWatch(cfg, opts)
				if err != nil {
					return nil, err
				}
				return interceptor.NewClient(c, interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						// Node event mappers also list drivers. Inject the error
						// into the resolver, so only its queued retry can recover.
						if _, ok := list.(*rebellionsaiv1alpha1.RBLNDriverList); ok && runtimecontroller.ReconcileIDFromContext(ctx) != "" {
							passes.Add(1)
							if failure == "list" && failed.CompareAndSwap(false, true) {
								return kapierrors.NewServiceUnavailable("injected driver list failure")
							}
						}
						return c.List(ctx, list, opts...)
					},
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if failure == "patch" && obj.GetName() == nodeNames[1] && failed.CompareAndSwap(false, true) {
							partialBeforeFailure.Store(successfulPatches.Load() > 0)
							return kapierrors.NewServiceUnavailable("injected owner patch failure")
						}
						err := c.Patch(ctx, obj, patch, opts...)
						if err == nil {
							successfulPatches.Add(1)
						}
						return err
					},
				}), nil
			},
		})
		Expect(err).NotTo(HaveOccurred())
		r := &RBLNDriverReconciler{
			Client:     mgr.GetClient(),
			APIReader:  mgr.GetAPIReader(),
			Log:        ctrl.Log.WithName("startup-test"),
			Scheme:     mgr.GetScheme(),
			Conditions: conditions.NewUpdater(mgr.GetClient()),
		}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		managerCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- mgr.Start(managerCtx) }()
		DeferCleanup(func() {
			cancel()
			Eventually(done, 10*time.Second).Should(Receive(BeNil()))
		})
		cacheCtx, cacheCancel := context.WithTimeout(managerCtx, 5*time.Second)
		defer cacheCancel()
		Expect(mgr.GetCache().WaitForCacheSync(cacheCtx)).To(BeTrue())

		Expect(k8sClient.Delete(ctx, driver)).To(Succeed())
		drivers := &rebellionsaiv1alpha1.RBLNDriverList{}
		Expect(k8sClient.List(ctx, drivers)).To(Succeed())
		Expect(drivers.Items).To(BeEmpty())
		Consistently(successfulPatches.Load, 300*time.Millisecond, 25*time.Millisecond).Should(BeZero())
		Expect(mgr.Elected()).NotTo(Receive())

		By("acquiring leadership after the last driver was deleted")
		Expect(k8sClient.Delete(ctx, lease)).To(Succeed())
		for _, name := range nodeNames {
			Eventually(func(g Gomega) {
				node := &corev1.Node{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
				g.Expect(node.Labels).NotTo(HaveKey(consts.RBLNDriverOwnerLabelKey))
				g.Expect(node.Labels).To(HaveKeyWithValue("test.rebellions.ai/keep", "unchanged"))
			}, 10*time.Second, 25*time.Millisecond).Should(Succeed())
		}
		if failure != "" {
			Expect(failed.Load()).To(BeTrue())
			Expect(passes.Load()).To(BeNumerically(">=", 2))
		}
		if failure == "patch" {
			Expect(partialBeforeFailure.Load()).To(BeTrue())
		}
		// Resolver failures must retry in the worker, not stop the manager.
		Expect(done).NotTo(Receive())
	},
		Entry("without API errors", ""),
		Entry("after a transient driver list failure", "list"),
		Entry("after a partially applied sweep", "patch"),
	)
})
