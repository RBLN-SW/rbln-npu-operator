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
	"maps"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
)

var _ = Describe("RBLNDriver Controller", Ordered, func() {
	var (
		ctx      context.Context
		nodeName string
	)

	BeforeAll(func() {
		ctx = context.Background()
		nodeName = fmt.Sprintf("driver-worker-%d", GinkgoParallelProcess())

		By("creating shared test node with NFD and driver deploy labels")
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"rebellions.ai/npu.present":       "true",
					"rebellions.ai/npu.deploy.driver": "true",
					// No hand-stamped owner label: the resolver now runs inside
					// Reconcile and stamps it from matching selectors; this family
					// label is what test-driver's selector matches.
					"rebellions.ai/npu.family":                                "atom",
					"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
					"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
					"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
				},
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
	})

	AfterAll(func() {
		By("deleting test node")
		node := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err == nil {
			_ = k8sClient.Delete(ctx, node)
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &corev1.Node{})
			}, 5*time.Second, 200*time.Millisecond).ShouldNot(Succeed(),
				"expected node %s to be deleted", nodeName)
		}
	})

	Context("When reconciling with valid dependencies", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("driver-cp"))
			driver := newDriverFixture("test-driver")
			// Matches the shared node's family label: the resolver stamps the
			// owner label from this selector and the DaemonSet then routes by
			// that owner label, not by the user selector.
			driver.Spec.NodeSelector = map[string]string{"rebellions.ai/npu.family": "atom"}
			nn = createDriverFixture(ctx, driver)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the driver")
			reconcileDriver(ctx, reconciler, nn)
		})

		It("creates driver manager resources", func() {
			By("stamping the owner label on the matching node")
			expectNodeLabel(ctx, nodeName, consts.RBLNDriverOwnerLabelKey, "test-driver")

			By("creating the driver ServiceAccount")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-driver", driverNS, 5*time.Second)

			By("creating the driver ClusterRole")
			expectClusterResource(ctx, &rbacv1.ClusterRole{}, "rbln-driver", 5*time.Second)

			By("creating the driver ClusterRoleBinding")
			expectClusterResource(ctx, &rbacv1.ClusterRoleBinding{}, "rbln-driver", 5*time.Second)

			By("creating the startup probe ConfigMap")
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-driver-startup-probe", driverNS, 5*time.Second)

			By("creating a DaemonSet for the detected node pool")
			expectResource(ctx, &appsv1.DaemonSet{}, "test-driver-atom-ubuntu22.04-5.15.0-100-generic", driverNS, 5*time.Second)
		})
	})

	// MissingClusterPolicy and InvalidSpec are covered end to end (condition +
	// event + metrics) by the "CR event contract" suite in cr_events_test.go.

	Context("When the driver resource does not exist", func() {
		It("returns no error", func() {
			reconciler := newTestDriverReconciler("")
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent-driver"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Context("When cluster-scoped resources are created", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-ownerref")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("ownerref-cp"))
			nn = createDriverFixture(ctx, newDriverFixture("ownerref-driver"))
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("should set ownerReferences on cluster-scoped resources for GC", func() {
			By("reconciling to create resources")
			reconcileDriver(ctx, reconciler, nn)

			By("verifying the ClusterRole has an ownerReference to the driver")
			var cr rbacv1.ClusterRole
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rbln-driver"}, &cr)).To(Succeed())
			Expect(cr.OwnerReferences).To(HaveLen(1))
			Expect(cr.OwnerReferences[0].Name).To(Equal(nn.Name))
			Expect(cr.OwnerReferences[0].Kind).To(Equal("RBLNDriver"))

			By("verifying the ClusterRoleBinding has an ownerReference to the driver")
			var crb rbacv1.ClusterRoleBinding
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rbln-driver"}, &crb)).To(Succeed())
			Expect(crb.OwnerReferences).To(HaveLen(1))
			Expect(crb.OwnerReferences[0].Name).To(Equal(nn.Name))
		})
	})

	Context("When running on OpenShift", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-ocp")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("v4.14.0")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("ocp-driver-cp"))
			nn = createDriverFixture(ctx, newDriverFixture("ocp-driver"))
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the driver")
			reconcileDriver(ctx, reconciler, nn)
		})

		It("creates OpenShift-specific RBAC resources", func() {
			By("creating the driver Role for SCC access")
			expectResource(ctx, &rbacv1.Role{}, "rbln-driver", driverNS, 5*time.Second)

			By("creating the driver RoleBinding for SCC access")
			expectResource(ctx, &rbacv1.RoleBinding{}, "rbln-driver", driverNS, 5*time.Second)
		})
	})

	Context("When two drivers use identical selectors", func() {
		var (
			reconciler *RBLNDriverReconciler
			spy        *crSpyRecorder
			tieNode    string
			nnA, nnB   types.NamespacedName
		)

		BeforeEach(func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-tie"))
			reconciler = newTestDriverReconciler("")
			spy = &crSpyRecorder{}
			reconciler.Recorder = spy
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("tie-cp"))

			// A dedicated node keeps the tie disjoint from the shared node's
			// family label, so this scenario cannot leak into other contexts.
			tieNode = fmt.Sprintf("tie-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, tieNode, map[string]string{"rebellions.ai/test-pool": "tie"})

			driverA := newDriverFixture("tie-driver-a")
			driverA.Spec.NodeSelector = map[string]string{"rebellions.ai/test-pool": "tie"}
			nnA = createDriverFixture(ctx, driverA)
			driverB := newDriverFixture("tie-driver-b")
			driverB.Spec.NodeSelector = map[string]string{"rebellions.ai/test-pool": "tie"}
			nnB = createDriverFixture(ctx, driverB)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("reports ConflictingNodeSelector on both drivers without freezing either", func() {
			By("both reconciles complete instead of erroring out")
			failedBefore := testutil.ToFloat64(metrics.ReconcileFailed.WithLabelValues("driver"))
			Expect(reconcileDriverWithResult(ctx, reconciler, nnA)).To(Equal(ctrl.Result{}))
			Expect(countEventReasons(spy, consts.RBLNConditionReasonConflictingSelector)).To(Equal(1))
			Expect(reconcileDriverWithResult(ctx, reconciler, nnB)).To(Equal(ctrl.Result{}))
			Expect(countEventReasons(spy, consts.RBLNConditionReasonConflictingSelector)).To(Equal(2))
			Expect(testutil.ToFloat64(metrics.ReconcileFailed.WithLabelValues("driver"))).To(Equal(failedBefore+2),
				"each conflicted reconcile must count as a non-ready outcome")

			By("both drivers report the tied node in their Ready condition")
			expectDriverNotReadyCondition(ctx, nnA, consts.RBLNConditionReasonConflictingSelector)
			expectDriverNotReadyCondition(ctx, nnB, consts.RBLNConditionReasonConflictingSelector)
			expectDriverReadyMessageContains(ctx, nnA, tieNode)
			expectDriverReadyMessageContains(ctx, nnB, tieNode)

			By("the tied node stays unowned until selectors are disambiguated")
			expectNodeHasNoLabel(ctx, tieNode, consts.RBLNDriverOwnerLabelKey)

			By("repeat reconciles of the unchanged specs emit no further Warnings")
			reconcileDriver(ctx, reconciler, nnA)
			reconcileDriver(ctx, reconciler, nnB)
			Expect(countEventReasons(spy, consts.RBLNConditionReasonConflictingSelector)).To(Equal(2))
		})
	})

	Context("When a fallback and a specific driver coexist", func() {
		var (
			reconciler             *RBLNDriverReconciler
			familyNode, plainNode  string
			nnFallback, nnSpecific types.NamespacedName
		)

		BeforeEach(func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-split"))
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("split-cp"))

			familyNode = fmt.Sprintf("split-family-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, familyNode, map[string]string{"rebellions.ai/test-family": "split"})
			plainNode = fmt.Sprintf("split-plain-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, plainNode, nil)

			nnFallback = createDriverFixture(ctx, newDriverFixture("split-fallback"))
			specific := newDriverFixture("split-specific")
			specific.Spec.NodeSelector = map[string]string{"rebellions.ai/test-family": "split"}
			nnSpecific = createDriverFixture(ctx, specific)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("routes each node to its most specific matching driver", func() {
			reconcileDriver(ctx, reconciler, nnFallback)
			reconcileDriver(ctx, reconciler, nnSpecific)

			By("the family node belongs to the specific driver")
			expectNodeLabel(ctx, familyNode, consts.RBLNDriverOwnerLabelKey, "split-specific")

			By("the plain node falls back to the empty-selector driver")
			expectNodeLabel(ctx, plainNode, consts.RBLNDriverOwnerLabelKey, "split-fallback")

			By("neither driver reports a selector conflict; both wait on pool rollout")
			expectDriverNotReadyCondition(ctx, nnFallback, consts.RBLNConditionReasonDriverPoolNotReady)
			expectDriverNotReadyCondition(ctx, nnSpecific, consts.RBLNConditionReasonDriverPoolNotReady)
		})
	})

	Context("When an owning driver is deleted", func() {
		var (
			reconciler             *RBLNDriverReconciler
			releaseNode            string
			nnFallback, nnSpecific types.NamespacedName
		)

		BeforeEach(func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-release"))
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("release-cp"))

			releaseNode = fmt.Sprintf("release-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, releaseNode, map[string]string{"rebellions.ai/test-release": "true"})

			nnFallback = createDriverFixture(ctx, newDriverFixture("release-fallback"))
			specific := newDriverFixture("release-specific")
			specific.Spec.NodeSelector = map[string]string{"rebellions.ai/test-release": "true"}
			nnSpecific = createDriverFixture(ctx, specific)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("reassigns the released node through the NotFound path", func() {
			By("the initial resolve routes the node to the specific driver")
			reconcileDriver(ctx, reconciler, nnSpecific)
			expectNodeLabel(ctx, releaseNode, consts.RBLNDriverOwnerLabelKey, "release-specific")

			By("deleting the specific driver and reconciling its stale request")
			var specific rebellionsaiv1alpha1.RBLNDriver
			Expect(k8sClient.Get(ctx, nnSpecific, &specific)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &specific)).To(Succeed())
			Expect(kapierrors.IsNotFound(
				k8sClient.Get(ctx, nnSpecific, &rebellionsaiv1alpha1.RBLNDriver{}))).To(BeTrue())
			reconcileDriver(ctx, reconciler, nnSpecific)

			By("the node is handed over to the fallback driver")
			expectNodeLabel(ctx, releaseNode, consts.RBLNDriverOwnerLabelKey, "release-fallback")

			By("the deletion pass reports exactly the survivor's owned-nodes series")
			// 2 = the release node plus the Describe-shared node; the fallback's
			// empty selector matches both. The series comes from the NotFound
			// pass itself, which must publish its resolve result.
			Expect(testutil.CollectAndCompare(metrics.DriverOwnedNodes, strings.NewReader(`
# HELP rbln_operator_driver_owned_nodes Number of nodes owned by each RBLNDriver instance.
# TYPE rbln_operator_driver_owned_nodes gauge
rbln_operator_driver_owned_nodes{driver="release-fallback"} 2
`))).To(Succeed())

			By("deleting the last driver leaves every in-domain node uncovered")
			var fallback rebellionsaiv1alpha1.RBLNDriver
			Expect(k8sClient.Get(ctx, nnFallback, &fallback)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &fallback)).To(Succeed())
			reconcileDriver(ctx, reconciler, nnFallback)
			expectNodeHasNoLabel(ctx, releaseNode, consts.RBLNDriverOwnerLabelKey)
			Expect(testutil.ToFloat64(metrics.DriverUncoveredNodes)).To(BeNumerically("==", 2),
				"the release node and the shared node lose coverage once no driver remains")
			Expect(testutil.CollectAndCount(metrics.DriverOwnedNodes)).To(BeZero(),
				"no candidates remain, so no owned-nodes series may survive")
		})
	})

	Context("When a node is relabeled to another driver's family", func() {
		var (
			reconciler      *RBLNDriverReconciler
			driverNS        string
			relabelNode     string
			nnAlpha, nnBeta types.NamespacedName
			alphaDS, betaDS string
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-relabel")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("relabel-cp"))

			relabelNode = fmt.Sprintf("relabel-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, relabelNode, map[string]string{"rebellions.ai/test-relabel-family": "alpha"})

			alpha := newDriverFixture("relabel-alpha")
			alpha.Spec.NodeSelector = map[string]string{"rebellions.ai/test-relabel-family": "alpha"}
			nnAlpha = createDriverFixture(ctx, alpha)
			beta := newDriverFixture("relabel-beta")
			beta.Spec.NodeSelector = map[string]string{"rebellions.ai/test-relabel-family": "beta"}
			nnBeta = createDriverFixture(ctx, beta)
			alphaDS = "relabel-alpha-atom-ubuntu22.04-5.15.0-100-generic"
			betaDS = "relabel-beta-atom-ubuntu22.04-5.15.0-100-generic"
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("moves ownership and the DaemonSet scope with the label", func() {
			By("the alpha driver owns the node and its pool DaemonSet targets it")
			reconcileDriver(ctx, reconciler, nnAlpha)
			reconcileDriver(ctx, reconciler, nnBeta)
			expectNodeLabel(ctx, relabelNode, consts.RBLNDriverOwnerLabelKey, "relabel-alpha")
			var ds appsv1.DaemonSet
			expectResource(ctx, &ds, alphaDS, driverNS, 5*time.Second)
			Expect(ds.Spec.Template.Spec.NodeSelector).To(
				HaveKeyWithValue(consts.RBLNDriverOwnerLabelKey, "relabel-alpha"))

			By("relabeling the live node to family beta")
			var node corev1.Node
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: relabelNode}, &node)).To(Succeed())
			node.Labels["rebellions.ai/test-relabel-family"] = "beta"
			Expect(k8sClient.Update(ctx, &node)).To(Succeed())

			// No manager runs here, so the direct Reconcile calls stand in for
			// the node watch's fan-out.
			By("reconciling both drivers as the node watch would")
			reconcileDriver(ctx, reconciler, nnBeta)
			reconcileDriver(ctx, reconciler, nnAlpha)

			By("ownership flips to the beta driver")
			expectNodeLabel(ctx, relabelNode, consts.RBLNDriverOwnerLabelKey, "relabel-beta")

			By("the beta DaemonSet selects the node by owner label")
			expectResource(ctx, &ds, betaDS, driverNS, 5*time.Second)
			Expect(ds.Spec.Template.Spec.NodeSelector).To(
				HaveKeyWithValue(consts.RBLNDriverOwnerLabelKey, "relabel-beta"))

			By("the alpha DaemonSet leaves scope and is cleaned up")
			// The reap is a foreground delete, so the DaemonSet lingers until
			// its pods are gone (the suite's emulateForegroundGC stands in for
			// kube-controller-manager) and alpha's status reads "pools
			// progressing" until then.
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, alphaDS, driverNS, 5*time.Second)

			By("the DS-deletion watch event drives one more reconcile; alpha reports it owns nothing")
			// In production Owns(&appsv1.DaemonSet{}) delivers this reconcile;
			// without a manager the direct call stands in for it.
			reconcileDriver(ctx, reconciler, nnAlpha)
			expectDriverReadyMessageContains(ctx, nnAlpha, "owns no nodes")
		})
	})

	Context("Node transition events", func() {
		var (
			reconciler *RBLNDriverReconciler
			spy        *crSpyRecorder
			eventNode  string
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-nodeevents"))
			reconciler = newTestDriverReconciler("")
			spy = &crSpyRecorder{}
			reconciler.Recorder = spy
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("nodeevents-cp"))

			eventNode = fmt.Sprintf("nodeevent-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, eventNode, map[string]string{"rebellions.ai/test-events": "true"})

			fixture := newDriverFixture("nodeevent-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/test-events": "true"}
			nn = createDriverFixture(ctx, fixture)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("emits owner transitions on the Node object and stays silent at steady state", func() {
			By("the first assignment emits DriverOwnerChanged (Normal) naming the owner")
			reconcileDriver(ctx, reconciler, nn)
			events := nodeEventsFor(spy, eventNode)
			Expect(events).To(HaveLen(1))
			Expect(events[0].eventType).To(Equal(corev1.EventTypeNormal))
			Expect(events[0].reason).To(Equal(consts.RBLNEventReasonDriverOwnerChanged))
			Expect(events[0].message).To(ContainSubstring("nodeevent-driver"))
			Expect(events[0].node.UID).NotTo(BeEmpty(),
				"the event must reference the live Node -- kubectl describe correlates events by involvedObject.uid")

			By("a steady-state reconcile emits no additional node events")
			before := countNodeEvents(spy)
			reconcileDriver(ctx, reconciler, nn)
			Expect(countNodeEvents(spy)).To(Equal(before),
				"owner events are transition-only; steady state must stay silent")

			By("losing the only matching driver emits DriverNodeUncovered (Warning) on the Node")
			var fixture rebellionsaiv1alpha1.RBLNDriver
			Expect(k8sClient.Get(ctx, nn, &fixture)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &fixture)).To(Succeed())
			reconcileDriver(ctx, reconciler, nn)
			events = nodeEventsFor(spy, eventNode)
			Expect(events).To(HaveLen(2))
			Expect(events[1].eventType).To(Equal(corev1.EventTypeWarning))
			Expect(events[1].reason).To(Equal(consts.RBLNEventReasonDriverNodeUncovered))
			expectNodeHasNoLabel(ctx, eventNode, consts.RBLNDriverOwnerLabelKey)
		})
	})

	Context("When the driver image is missing from the registry", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			spy        *crSpyRecorder
			imgNode    string
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-imgmissing")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			// "atom" is the shared family every fixture in this suite uses, and
			// GetPrecompiledImagePath always splices it into the composed
			// image path -- a distinctive, default-independent substring for
			// this driver's one pool.
			reconciler.imageChecker = &fakeImageChecker{verdicts: map[string]registry.Verdict{"atom": registry.VerdictNotFound}}
			spy = &crSpyRecorder{}
			reconciler.Recorder = spy
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("imgmissing-cp"))

			imgNode = fmt.Sprintf("imgmissing-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, imgNode, map[string]string{"rebellions.ai/test-imgmissing": "true"})

			fixture := newDriverFixture("imgmissing-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/test-imgmissing": "true"}
			nn = createDriverFixture(ctx, fixture)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("reports DriverImageNotFound and polls every 5 minutes until it clears", func() {
			By("the first reconcile reports Ready=False naming the image ref, and polls")
			result := reconcileDriverWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: registry.NegativeTTL}))
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonImageNotFound)
			expectDriverReadyMessageContains(ctx, nn, "atom")
			Expect(countEventReasons(spy, consts.RBLNConditionReasonImageNotFound)).To(Equal(1))

			By("no DaemonSet exists for the pool with the missing image")
			dsList := &appsv1.DaemonSetList{}
			Expect(k8sClient.List(ctx, dsList, client.InNamespace(driverNS))).To(Succeed())
			Expect(dsList.Items).To(BeEmpty())

			By("a repeat reconcile of the unchanged spec emits no further Warning")
			result = reconcileDriverWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: registry.NegativeTTL}))
			Expect(countEventReasons(spy, consts.RBLNConditionReasonImageNotFound)).To(Equal(1))
		})

		It("self-heals once the image is published", func() {
			By("the image is still missing on the first reconcile")
			reconcileDriver(ctx, reconciler, nn)
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonImageNotFound)

			By("the image becomes available and the pool's DaemonSet gets created")
			reconciler.imageChecker = &fakeImageChecker{}
			reconcileDriver(ctx, reconciler, nn)
			dsName := "imgmissing-driver-atom-ubuntu22.04-5.15.0-100-generic"
			var ds appsv1.DaemonSet
			expectResource(ctx, &ds, dsName, driverNS, 5*time.Second)

			// envtest runs no DaemonSet controller, so a freshly created
			// DaemonSet's status never reports scheduled/ready pods on its
			// own; simulate what that controller would do once pods land.
			By("the DaemonSet's pods become ready and the driver reports Ready")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: dsName, Namespace: driverNS}, &ds)).To(Succeed())
			ds.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, NumberReady: 1}
			Expect(k8sClient.Status().Update(ctx, &ds)).To(Succeed())
			markSmdDaemonSetReady(ctx, "imgmissing-driver-smd", driverNS)
			reconcileDriver(ctx, reconciler, nn)
			expectDriverReadyCondition(ctx, nn)
		})
	})

	Context("rbln-smd companion DaemonSet", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			spy        *crSpyRecorder
			smdNode    string
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-smd")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			spy = &crSpyRecorder{}
			reconciler.Recorder = spy
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("smd-cp"))

			smdNode = fmt.Sprintf("smd-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, smdNode, map[string]string{"rebellions.ai/test-smd": "true"})

			fixture := newDriverFixture("smd-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/test-smd": "true"}
			nn = createDriverFixture(ctx, fixture)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("deploys smd with the driver, reports SmdNotReady without an event, then Ready", func() {
			By("one reconcile renders the driver pool and the smd DaemonSet in the same pass")
			reconcileDriver(ctx, reconciler, nn)
			driverDSName := "smd-driver-atom-ubuntu22.04-5.15.0-100-generic"
			var driverDS appsv1.DaemonSet
			expectResource(ctx, &driverDS, driverDSName, driverNS, 5*time.Second)
			var smdDS appsv1.DaemonSet
			expectResource(ctx, &smdDS, "smd-driver-smd", driverNS, 5*time.Second)

			By("the smd DaemonSet must stay invisible to the upgrade controller's component scan")
			Expect(smdDS.Labels["app.kubernetes.io/component"]).To(Equal("rbln-smd"))
			Expect(smdDS.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteDaemonSetStrategyType))
			Expect(smdDS.Spec.Template.Spec.Containers[0].Image).To(HaveSuffix(":" + newDriverFixture("x").Spec.Version))

			By("driver pool ready but smd catching up reports SmdNotReady with no event")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: driverDSName, Namespace: driverNS}, &driverDS)).To(Succeed())
			driverDS.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, NumberReady: 1}
			Expect(k8sClient.Status().Update(ctx, &driverDS)).To(Succeed())
			result := reconcileDriverWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: 5 * time.Second}))
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonSmdNotReady)
			Expect(countEventReasons(spy, consts.RBLNConditionReasonSmdNotReady)).To(Equal(0))
			// The smd DS status is still zero-valued here (Desired==0): no
			// owned node carries the deploy label yet, so the condition must
			// point at the label instead of "0 of 0 pods are Ready".
			expectDriverReadyMessageContains(ctx, nn, "no eligible nodes")

			By("smd pods become ready and the CR reports Ready with status.smd populated")
			markSmdDaemonSetReady(ctx, "smd-driver-smd", driverNS)
			reconcileDriver(ctx, reconciler, nn)
			expectDriverReadyCondition(ctx, nn)
			var driver rebellionsaiv1alpha1.RBLNDriver
			Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
			Expect(driver.Status.Smd).NotTo(BeNil())
			Expect(driver.Status.Smd.State).To(Equal(rebellionsaiv1alpha1.DriverPoolStateReady))
			// smd counts stay out of the driver-node sums (external contract).
			Expect(driver.Status.DesiredNodes).To(Equal(int32(1)))
			Expect(driver.Status.ReadyNodes).To(Equal(int32(1)))
		})
	})

	Context("When an owned node lacks the family label", func() {
		var (
			driverNS     string
			reconciler   *RBLNDriverReconciler
			spy          *crSpyRecorder
			noFamilyNode string
			nn           types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-nofamily")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			spy = &crSpyRecorder{}
			reconciler.Recorder = spy
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("nofamily-cp"))

			noFamilyNode = fmt.Sprintf("nofamily-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, noFamilyNode, map[string]string{"rebellions.ai/test-nofamily": "true"})
			// createDriverPoolNode defaults npu.family to "atom"; strip it so
			// this node is owned (the selector below still matches it) but
			// carries no usable family label.
			removeNodeLabel(ctx, noFamilyNode, consts.RBLNNPUFamilyLabelKey)

			fixture := newDriverFixture("nofamily-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/test-nofamily": "true"}
			nn = createDriverFixture(ctx, fixture)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("reports DriverFamilyLabelMissing with no requeue, then recovers once labeled", func() {
			By("the owned node has no usable family label")
			result := reconcileDriverWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{}))
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonFamilyLabelMissing)
			expectDriverReadyMessageContains(ctx, nn, noFamilyNode)
			Expect(countEventReasons(spy, consts.RBLNConditionReasonFamilyLabelMissing)).To(Equal(1))

			By("a repeat reconcile of the unchanged spec emits no further Warning")
			reconcileDriver(ctx, reconciler, nn)
			Expect(countEventReasons(spy, consts.RBLNConditionReasonFamilyLabelMissing)).To(Equal(1))

			// The owner label is set from the selector alone, independent of
			// family, so it is already present before this point; what the
			// family label actually gates is pool discovery, proven below by
			// the pool's DaemonSet coming into existence.
			By("labeling the node with a family lets a pool -- and its DaemonSet -- be created")
			setNodeLabel(ctx, noFamilyNode, consts.RBLNNPUFamilyLabelKey, "atom")
			reconcileDriver(ctx, reconciler, nn)
			expectResource(ctx, &appsv1.DaemonSet{}, "nofamily-driver-atom-ubuntu22.04-5.15.0-100-generic", driverNS, 5*time.Second)
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonDriverPoolNotReady)
		})
	})

	// With both fast-fail diagnostics at once, FamilyLabelMissing wins the
	// reason but the image-recovery poll must still fire: a permanently
	// family-less node would otherwise pin the CR forever and swallow a
	// later image publish on its labeled sibling.
	Context("When a family-less node and an image-missing sibling coexist", func() {
		var (
			reconciler       *RBLNDriverReconciler
			noFamilyNode     string
			imageMissingNode string
			nn               types.NamespacedName
		)

		BeforeEach(func() {
			GinkgoT().Setenv("OPERATOR_NAMESPACE", createTestNamespace(ctx, "rbln-driver-mixed"))
			reconciler = newTestDriverReconciler("")
			// imageMissingNode's composed ref always contains "atom" (its
			// family); noFamilyNode produces no pool at all, so it never
			// reaches the checker -- both diagnostics still come from the
			// same Patch call.
			reconciler.imageChecker = &fakeImageChecker{verdicts: map[string]registry.Verdict{"atom": registry.VerdictNotFound}}
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("mixed-cp"))

			noFamilyNode = fmt.Sprintf("mixed-nofamily-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, noFamilyNode, map[string]string{"rebellions.ai/test-mixed": "true"})
			removeNodeLabel(ctx, noFamilyNode, consts.RBLNNPUFamilyLabelKey)

			imageMissingNode = fmt.Sprintf("mixed-imgmissing-worker-%d", GinkgoParallelProcess())
			createDriverPoolNode(ctx, imageMissingNode, map[string]string{"rebellions.ai/test-mixed": "true"})

			fixture := newDriverFixture("mixed-driver")
			fixture.Spec.NodeSelector = map[string]string{"rebellions.ai/test-mixed": "true"}
			nn = createDriverFixture(ctx, fixture)
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("reports DriverFamilyLabelMissing but still carries the image-recovery poll", func() {
			result := reconcileDriverWithResult(ctx, reconciler, nn)
			// Family wins the reason; the poll fires because a missing image
			// is present at all, not because of which reason won.
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: registry.NegativeTTL}))
			expectDriverNotReadyCondition(ctx, nn, consts.RBLNConditionReasonFamilyLabelMissing)
			expectDriverReadyMessageContains(ctx, nn, noFamilyNode)
			expectDriverReadyMessageContains(ctx, nn, "atom")
		})
	})
})

// ---------------------------------------------------------------------------
// Reconciler factory
// ---------------------------------------------------------------------------

// fakeImageChecker stands in for the real Checker in tests that construct the
// reconciler directly. Its zero value always reports VerdictExists, so only
// scenarios that need another verdict have to set one.
type fakeImageChecker struct {
	verdicts map[string]registry.Verdict
}

var _ components.ImageChecker = &fakeImageChecker{}

func (f *fakeImageChecker) Check(_ context.Context, imageRef string, _ []corev1.Secret) (registry.Verdict, error) {
	return f.verdictFor(imageRef), nil
}

// verdictFor picks the first configured key that is a substring of ref,
// falling back to VerdictExists. Matching by substring instead of the exact
// composed image path keeps tests decoupled from
// RBLNDriverSpec.GetPrecompiledImagePath's registry/image/version defaults.
func (f *fakeImageChecker) verdictFor(ref string) registry.Verdict {
	for key, v := range f.verdicts {
		if strings.Contains(ref, key) {
			return v
		}
	}
	return registry.VerdictExists
}

func newTestDriverReconciler(openShiftVersion string) *RBLNDriverReconciler {
	r := &RBLNDriverReconciler{
		Client:     k8sClient,
		APIReader:  k8sClient, // envtest client is uncached already
		Log:        logf.Log,
		Scheme:     k8sClient.Scheme(),
		Conditions: conditions.NewUpdater(k8sClient),
		// Mirrors SetupWithManager, which tests bypass by calling Reconcile
		// directly; Reconcile assumes the resolver is always present.
		ownerResolver: driver.NewOwnerResolver(k8sClient, logf.Log),
		imageChecker:  &fakeImageChecker{},
	}
	if openShiftVersion != "" {
		r.ClusterInfo = &clusterinfo.Info{
			OpenShiftVersion: openShiftVersion,
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

func newDriverTestClusterPolicy(name string) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType:        consts.RBLNWorkloadConfigContainer,
			DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{Enabled: false},
			NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{Enabled: false},
			MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{Enabled: false},
			VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: false},
			SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
		},
	}
}

func newDriverFixture(name string) *rebellionsaiv1alpha1.RBLNDriver {
	return &rebellionsaiv1alpha1.RBLNDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: rebellionsaiv1alpha1.RBLNDriverSpec{
			Version: "3.0.0",
		},
	}
}

func createDriverFixture(ctx context.Context, driver *rebellionsaiv1alpha1.RBLNDriver) types.NamespacedName {
	Expect(k8sClient.Create(ctx, driver)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, driver) })
	return types.NamespacedName{Name: driver.Name}
}

// createDriverPoolNode creates a node inside the resolver's scope (deploy
// gate + NFD labels) plus any scenario-specific labels, cleaned up per spec.
// Defaults to family "atom" so every existing scenario keeps producing pools;
// pass a "rebellions.ai/npu.family" entry in extraLabels to override it.
func createDriverPoolNode(ctx context.Context, name string, extraLabels map[string]string) {
	GinkgoHelper()
	labels := map[string]string{
		"rebellions.ai/npu.deploy.driver":                         "true",
		"rebellions.ai/npu.family":                                "atom",
		"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
		"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
		"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
	}
	maps.Copy(labels, extraLabels)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	Expect(k8sClient.Create(ctx, node)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, node) })
}

// ---------------------------------------------------------------------------
// Reconcile helpers
// ---------------------------------------------------------------------------

func reconcileDriver(ctx context.Context, reconciler *RBLNDriverReconciler, nn types.NamespacedName) {
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
	Expect(err).NotTo(HaveOccurred())
}

func reconcileDriverWithResult(
	ctx context.Context,
	reconciler *RBLNDriverReconciler,
	nn types.NamespacedName,
) ctrl.Result {
	result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
	Expect(err).NotTo(HaveOccurred())
	return result
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func expectDriverNotReadyCondition(ctx context.Context, nn types.NamespacedName, reason string) {
	Eventually(func() bool {
		var driver rebellionsaiv1alpha1.RBLNDriver
		Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
		for _, c := range driver.Status.Conditions {
			if c.Type == consts.RBLNConditionTypeReady && c.Status == metav1.ConditionFalse && c.Reason == reason {
				return true
			}
		}
		return false
	}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(),
		"expected Driver Ready condition status=%s reason=%s", metav1.ConditionFalse, reason)
}

func expectDriverReadyCondition(ctx context.Context, nn types.NamespacedName) {
	Eventually(func() bool {
		var driver rebellionsaiv1alpha1.RBLNDriver
		Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
		for _, c := range driver.Status.Conditions {
			if c.Type == consts.RBLNConditionTypeReady && c.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(),
		"expected Driver Ready condition status=%s", metav1.ConditionTrue)
}

// expectDriverReadyMessageContains asserts on the settled Ready condition
// message; call it after expectDriverNotReadyCondition has waited for status.
func expectDriverReadyMessageContains(ctx context.Context, nn types.NamespacedName, substr string) {
	GinkgoHelper()
	var driver rebellionsaiv1alpha1.RBLNDriver
	Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
	for _, c := range driver.Status.Conditions {
		if c.Type == consts.RBLNConditionTypeReady {
			Expect(c.Message).To(ContainSubstring(substr))
			return
		}
	}
	Fail(fmt.Sprintf("Ready condition not found on RBLNDriver %s", nn.Name))
}

// countEventReasons filters by reason because reportResolution interleaves
// node-scoped events with the CR-scoped ones a scenario asserts on.
// markSmdDaemonSetReady simulates the DaemonSet controller for the rbln-smd
// DaemonSet, which envtest does not run.
func markSmdDaemonSetReady(ctx context.Context, name, namespace string) {
	GinkgoHelper()
	var ds appsv1.DaemonSet
	expectResource(ctx, &ds, name, namespace, 5*time.Second)
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &ds)).To(Succeed())
	ds.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, NumberReady: 1}
	Expect(k8sClient.Status().Update(ctx, &ds)).To(Succeed())
}

func countEventReasons(spy *crSpyRecorder, reason string) int {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	count := 0
	for _, r := range spy.reasons {
		if r == reason {
			count++
		}
	}
	return count
}

// nodeEvent flattens one spy entry for Node-scoped assertions. node is the
// involvedObject itself, so assertions can reach fields the flattened strings
// drop (UID, labels).
type nodeEvent struct {
	eventType string
	reason    string
	message   string
	node      *corev1.Node
}

// nodeEventsFor returns the spy's events whose involvedObject is the named
// Node, in emission order. Filtering by node name keeps assertions immune to
// transitions on other nodes (e.g. the Describe-shared node) in the same pass.
func nodeEventsFor(spy *crSpyRecorder, nodeName string) []nodeEvent {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	events := make([]nodeEvent, 0, len(spy.objects))
	for i, obj := range spy.objects {
		node, ok := obj.(*corev1.Node)
		if !ok || node.Name != nodeName {
			continue
		}
		events = append(events, nodeEvent{
			eventType: spy.types[i],
			reason:    spy.reasons[i],
			message:   spy.messages[i],
			node:      node,
		})
	}
	return events
}

// countNodeEvents counts spy events whose involvedObject is any Node.
func countNodeEvents(spy *crSpyRecorder) int {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	count := 0
	for _, obj := range spy.objects {
		if _, ok := obj.(*corev1.Node); ok {
			count++
		}
	}
	return count
}

func expectClusterResource[T client.Object](ctx context.Context, obj T, name string, timeout time.Duration) {
	Eventually(func() error {
		return k8sClient.Get(ctx, types.NamespacedName{Name: name}, obj)
	}, timeout, 250*time.Millisecond).Should(Succeed(), "expected %T %s to exist", obj, name)
}

// ---------------------------------------------------------------------------
// Cleanup helpers
// ---------------------------------------------------------------------------

func cleanupDriverClusterRBAC(ctx context.Context) {
	_ = k8sClient.Delete(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "rbln-driver"}})
	_ = k8sClient.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "rbln-driver"}})
}
