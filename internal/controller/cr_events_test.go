package controller

import (
	"context"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// crSpyRecorder captures involvedObject, which FakeRecorder cannot.
type crSpyRecorder struct {
	mu      sync.Mutex
	objects []runtime.Object
	reasons []string
}

func (s *crSpyRecorder) Event(object runtime.Object, _, reason, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = append(s.objects, object)
	s.reasons = append(s.reasons, reason)
}

func (s *crSpyRecorder) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...interface{}) {
	s.Event(object, eventType, reason, fmt.Sprintf(messageFmt, args...))
}

func (s *crSpyRecorder) AnnotatedEventf(object runtime.Object, _ map[string]string, eventType, reason, messageFmt string, args ...interface{}) {
	s.Eventf(object, eventType, reason, messageFmt, args...)
}

var _ = Describe("CR event contract", Ordered, func() {
	ctx := context.Background()

	Context("RBLNDriver events", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-events")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("driver-events-cp"))
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("emits DriverReady once on transition, with the driver as involvedObject", func() {
			spy := &crSpyRecorder{}
			reconciler.Recorder = spy

			// A selector matching no node keeps every pool empty → Ready path.
			fixture := newDriverFixture("ready-event-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/no-such-node": "true"}
			nn := createDriverFixture(ctx, fixture)

			By("first Ready transition emits exactly one DriverReady on the driver CR")
			reconcileDriver(ctx, reconciler, nn)
			Expect(spy.reasons).To(Equal([]string{consts.RBLNEventReasonDriverReady}))
			driver, ok := spy.objects[0].(*rebellionsaiv1alpha1.RBLNDriver)
			Expect(ok).To(BeTrue(), "involvedObject must be the RBLNDriver")
			Expect(driver.Name).To(Equal("ready-event-driver"))

			By("repeated Ready reconcile emits nothing")
			reconcileDriver(ctx, reconciler, nn)
			Expect(spy.reasons).To(HaveLen(1))
		})

		It("emits DriverInstallFailed when component apply fails", func() {
			recorder := record.NewFakeRecorder(8)
			reconciler.Recorder = recorder
			GinkgoT().Setenv("OPERATOR_NAMESPACE", "no-such-namespace-for-events")

			fixture := newDriverFixture("install-fail-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/no-such-node": "true"}
			nn := createDriverFixture(ctx, fixture)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred(), "apply failure must surface as reconcile error")
			expectPolicyEvent(recorder, corev1.EventTypeWarning, consts.RBLNEventReasonDriverInstallFailed)
			expectNoPolicyEvent(recorder)
		})
	})

	Context("RBLNDriver non-goal conditions", func() {
		It("emits nothing for MissingClusterPolicy", func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-nopolicy"))
			reconciler := newTestDriverReconciler("")
			recorder := record.NewFakeRecorder(8)
			reconciler.Recorder = recorder

			nn := createDriverFixture(ctx, newDriverFixture("orphan-driver"))
			reconcileDriverWithResult(ctx, reconciler, nn)

			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonMissingClusterPolicy)
			expectNoPolicyEvent(recorder)
		})
	})

	Context("RBLNClusterPolicy apply failure", func() {
		var reconciler *RBLNClusterPolicyReconciler

		BeforeEach(func() {
			// Namespace that does not exist forces PatchComponents to fail.
			GinkgoT().Setenv("OPERATOR_NAMESPACE", "no-such-namespace-for-events")
			reconciler = newTestClusterPolicyReconciler("")

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   "apply-fail-worker",
				Labels: map[string]string{consts.NFDDevicePCILabelKey: "true"},
			}}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, node)
			})
		})

		It("emits ComponentApplyFailed with the policy as involvedObject", func() {
			spy := &crSpyRecorder{}
			reconciler.Recorder = spy
			nn := createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("apply-fail-policy"))

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(spy.reasons).To(ContainElement(consts.RBLNEventReasonComponentApplyFailed))
			policy, ok := spy.objects[0].(*rblnv1beta1.RBLNClusterPolicy)
			Expect(ok).To(BeTrue(), "involvedObject must be the RBLNClusterPolicy")
			Expect(policy.Name).To(Equal("apply-fail-policy"))
		})
	})
})
