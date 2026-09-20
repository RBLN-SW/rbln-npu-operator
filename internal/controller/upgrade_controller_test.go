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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

// ---------------------------------------------------------------------------
// Mock StateManager
// ---------------------------------------------------------------------------

type mockStateManager struct {
	buildStateFunc func(ctx context.Context, namespace string, driverLabels map[string]string) (*upgrade.ClusterUpgradeState, error)
	applyStateFunc func(ctx context.Context, currentState *upgrade.ClusterUpgradeState, upgradePolicy *rblnv1beta1.DriverUpgradePolicySpec) error
}

func (m *mockStateManager) WithPodDeletionEnabled(_ upgrade.PodDeletionFilter) upgrade.ClusterUpgradeStateManager {
	return m
}

func (m *mockStateManager) WithValidationEnabled(_ string) upgrade.ClusterUpgradeStateManager {
	return m
}

func (m *mockStateManager) BuildState(ctx context.Context, namespace string, driverLabels map[string]string) (*upgrade.ClusterUpgradeState, error) {
	if m.buildStateFunc != nil {
		return m.buildStateFunc(ctx, namespace, driverLabels)
	}
	return &upgrade.ClusterUpgradeState{NodeStates: map[string][]*upgrade.NodeUpgradeState{}}, nil
}

func (m *mockStateManager) ApplyState(ctx context.Context, currentState *upgrade.ClusterUpgradeState, upgradePolicy *rblnv1beta1.DriverUpgradePolicySpec) error {
	if m.applyStateFunc != nil {
		return m.applyStateFunc(ctx, currentState, upgradePolicy)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

var _ = Describe("Upgrade Controller", Ordered, func() {
	var (
		ctx      context.Context
		nodeName string
	)

	BeforeAll(func() {
		ctx = context.Background()
		nodeName = fmt.Sprintf("upgrade-worker-%d", GinkgoParallelProcess())

		By("creating shared test node")
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   nodeName,
				Labels: map[string]string{},
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

	Context("When the ClusterPolicy does not exist", func() {
		It("returns no error and no requeue", func() {
			reconciler := newTestUpgradeReconciler(&mockStateManager{})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent-policy"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Context("When upgradePolicy is nil", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			reconciler = newTestUpgradeReconciler(&mockStateManager{})

			By("adding upgrade state label to the node")
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("nil-policy", nil))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("cleans upgrade state labels and does not requeue", func() {
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			By("verifying upgrade state label is removed from the node")
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
		})
	})

	Context("When autoUpgrade is false", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			reconciler = newTestUpgradeReconciler(&mockStateManager{})

			By("adding upgrade state label to the node")
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("disabled-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: false,
			}))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("cleans upgrade state labels and does not requeue", func() {
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			By("verifying upgrade state label is removed from the node")
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
		})

		// Turning autoUpgrade off is the documented way to pause a rollout, so
		// a node caught mid-flight must not be left unschedulable with the
		// state label that explained it gone.
		It("returns a mid-rollout node to service", func() {
			DeferCleanup(func() { restoreNodeSchedulable(ctx, nodeName) })
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStatePodRestartRequired)
			setNodeUnschedulable(ctx, nodeName, true)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			expectNodeUnschedulable(ctx, nodeName, false)
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
		})

		// A cordon predating the rollout belongs to whoever placed it. The
		// annotation that records this still goes, so the next rollout derives
		// it from the node instead of trusting a stale one.
		It("leaves a cordon the rollout did not take and drops its initial-state annotation", func() {
			DeferCleanup(func() { restoreNodeSchedulable(ctx, nodeName) })
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStatePodRestartRequired)
			setNodeUnschedulable(ctx, nodeName, true)
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeInitialStateAnnotationKey, "true")

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			expectNodeUnschedulable(ctx, nodeName, true)
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradeInitialStateAnnotationKey)
		})

		// A failed node runs a driver that never came up, so it stays isolated.
		// Its label stays too: without it the next rollout would admit the node
		// as unknown, read this rollout's cordon as the administrator's and
		// never lift it, wiping the failure reason on the way in.
		It("leaves an upgrade-failed node parked with its label, cordon and failure reason", func() {
			DeferCleanup(func() {
				restoreNodeSchedulable(ctx, nodeName)
				removeNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
				removeNodeAnnotation(ctx, nodeName, upgrade.UpgradeFailureReasonAnnotationKey)
			})
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStateFailed)
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeFailureReasonAnnotationKey, "driver pod crash-looping")
			setNodeUnschedulable(ctx, nodeName, true)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			expectNodeUnschedulable(ctx, nodeName, true)
			expectNodeLabelValue(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStateFailed)
			expectNodeKeepsAnnotation(ctx, nodeName, upgrade.UpgradeFailureReasonAnnotationKey)
		})

		// The timeout clocks belong to the rollout that started them; left
		// behind, the next rollout would read a stale epoch and time out at once.
		It("drops the rollout's timeout clocks", func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStateValidationRequired)
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeValidationStartTimeAnnotationKey, "1")
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeWaitForPodCompletionStartTimeAnnotationKey, "1")

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradeValidationStartTimeAnnotationKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradeWaitForPodCompletionStartTimeAnnotationKey)
		})

		// The attempt's judgement artifacts go with the label. Admission drops
		// them through clearParkedBookkeeping, but a node whose driver pod is
		// already in sync is moved straight to upgrade-done without passing it.
		It("drops the parked-node bookkeeping", func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, upgrade.UpgradeStateSkipped)
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradePodRestartStartTimeAnnotationKey, "1")
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeSkipReasonAnnotationKey, "pod eviction blocked")
			setNodeAnnotation(ctx, nodeName, upgrade.UpgradeAttemptedRevisionAnnotationKey, "digest-a")

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradePodRestartStartTimeAnnotationKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradeSkipReasonAnnotationKey)
			expectNodeHasNoAnnotation(ctx, nodeName, upgrade.UpgradeAttemptedRevisionAnnotationKey)
		})

		// The verdict comes from labels the state manager wrote through its own
		// direct client moments ago; the informer cache may not have caught up,
		// so the nodes must be read from the API server.
		It("reads the nodes from the API server, not the cache", func() {
			reconciler.Client = newInterceptedClient(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, isNodes := list.(*corev1.NodeList); isNodes {
						return errors.New("node list served from the cache")
					}
					return c.List(ctx, list, opts...)
				},
			})

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
		})

		// One node's patch failing must not leave the nodes behind it labeled
		// and cordoned until the requeue; each node is torn down on its own.
		It("keeps tearing down the other nodes when one patch fails", func() {
			// Sorts before the shared node, so the old loop would stop here.
			failingNode := fmt.Sprintf("a-teardown-fail-%d", GinkgoParallelProcess())
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   failingNode,
				Labels: map[string]string{upgrade.UpgradeStateLabelKey: upgrade.UpgradeStateUpgradeRequired},
			}}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })

			reconciler.Client = newInterceptedClient(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if obj.GetName() == failingNode {
						return errors.New("patch rejected")
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(MatchError(ContainSubstring(failingNode)))
			expectNodeHasNoLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey)
		})
	})

	Context("When BuildState returns a general error", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			mock := &mockStateManager{
				buildStateFunc: func(_ context.Context, _ string, _ map[string]string) (*upgrade.ClusterUpgradeState, error) {
					return nil, fmt.Errorf("unexpected build error")
				},
			}
			reconciler = newTestUpgradeReconciler(mock)

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("build-err-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("returns the error", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unexpected build error"))
		})
	})

	Context("When ApplyState returns an error", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			mock := &mockStateManager{
				applyStateFunc: func(_ context.Context, _ *upgrade.ClusterUpgradeState, _ *rblnv1beta1.DriverUpgradePolicySpec) error {
					return fmt.Errorf("apply failed")
				},
			}
			reconciler = newTestUpgradeReconciler(mock)

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("apply-err-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("returns the error", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("apply failed"))
		})
	})

	Context("When upgrade succeeds (happy path)", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			reconciler = newTestUpgradeReconciler(&mockStateManager{})

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("happy-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("requeues after planned interval (10s)", func() {
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: 10 * time.Second}))
		})

		It("publishes the driver upgrade status block and conditions", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			policy := &rblnv1beta1.RBLNClusterPolicy{}
			Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
			Expect(policy.Status.DriverUpgrade).NotTo(BeNil())
			Expect(policy.Status.DriverUpgrade.Progress).NotTo(BeEmpty())
			for _, conditionType := range upgradeConditionTypes {
				Expect(findCondition(policy.Status.Conditions, conditionType)).NotTo(BeNil(),
					"expected condition %s to be published", conditionType)
			}
		})
	})

	Context("When auto-upgrade is turned off after a rollout", func() {
		var (
			reconciler *UpgradeReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			reconciler = newTestUpgradeReconciler(&mockStateManager{})

			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("clear-status-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)

			By("publishing the status once while auto-upgrade is on")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			By("disabling auto-upgrade")
			policy := &rblnv1beta1.RBLNClusterPolicy{}
			Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
			policy.Spec.Driver.UpgradePolicy.AutoUpgrade = false
			Expect(k8sClient.Update(ctx, policy)).To(Succeed())
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)
		})

		It("clears the upgrade-owned status block and conditions", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			policy := &rblnv1beta1.RBLNClusterPolicy{}
			Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
			Expect(policy.Status.DriverUpgrade).To(BeNil())
			for _, conditionType := range upgradeConditionTypes {
				Expect(findCondition(policy.Status.Conditions, conditionType)).To(BeNil(),
					"expected condition %s to be removed", conditionType)
			}
		})
	})

	Context("When the policy status is not decided yet", func() {
		var nn types.NamespacedName

		BeforeEach(func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")
			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("pending-policy", nil))
		})

		It("requeues shortly and leaves upgrade state untouched", func() {
			reconciler := newTestUpgradeReconciler(&mockStateManager{})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: statusPendingRequeueInterval}))
			expectNodeKeepsUpgradeRequiredLabel(ctx, nodeName)
		})
	})

	Context("When the policy status is stale (old generation)", func() {
		var nn types.NamespacedName

		BeforeEach(func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")
			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("stale-gen-policy", nil))
			markClusterPolicyState(ctx, nn, consts.RBLNStateReady)

			By("bumping the spec so Generation moves past ObservedGeneration")
			policy := &rblnv1beta1.RBLNClusterPolicy{}
			Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
			policy.Spec.DevicePlugin.Enabled = !policy.Spec.DevicePlugin.Enabled
			Expect(k8sClient.Update(ctx, policy)).To(Succeed())
		})

		It("requeues shortly and leaves upgrade state untouched", func() {
			reconciler := newTestUpgradeReconciler(&mockStateManager{})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: statusPendingRequeueInterval}))
			expectNodeKeepsUpgradeRequiredLabel(ctx, nodeName)
		})
	})

	Context("When the policy is ignored (non-singleton)", func() {
		var nn types.NamespacedName

		BeforeEach(func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")
			nn = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("ignored-policy", nil))
			markClusterPolicyState(ctx, nn, consts.RBLNStateIgnored)
		})

		It("returns without touching upgrade state", func() {
			reconciler := newTestUpgradeReconciler(&mockStateManager{})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			expectNodeKeepsUpgradeRequiredLabel(ctx, nodeName)
		})
	})

	Context("When a deleted policy leaves an active one behind", func() {
		var activeNN types.NamespacedName

		BeforeEach(func() {
			setNodeLabel(ctx, nodeName, upgrade.UpgradeStateLabelKey, "upgrade-required")
			activeNN = createClusterPolicyFixture(ctx, newUpgradeClusterPolicyFixture("surviving-policy", &rblnv1beta1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}))
			markClusterPolicyState(ctx, activeNN, consts.RBLNStateReady)
		})

		It("does not clean cluster-wide upgrade state", func() {
			reconciler := newTestUpgradeReconciler(&mockStateManager{})
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "already-deleted-policy"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			expectNodeKeepsUpgradeRequiredLabel(ctx, nodeName)
		})
	})
})

// ---------------------------------------------------------------------------
// Reconciler factory
// ---------------------------------------------------------------------------

func newTestUpgradeReconciler(sm upgrade.ClusterUpgradeStateManager) *UpgradeReconciler {
	return &UpgradeReconciler{
		Client:       k8sClient,
		APIReader:    k8sClient,
		Scheme:       k8sClient.Scheme(),
		Namespace:    "test-namespace",
		StateManager: sm,
	}
}

// newInterceptedClient wraps a direct client so a test can fail selected calls.
func newInterceptedClient(funcs interceptor.Funcs) client.Client {
	GinkgoHelper()
	direct, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	return interceptor.NewClient(direct, funcs)
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

func expectNodeKeepsUpgradeRequiredLabel(ctx context.Context, nodeName string) {
	GinkgoHelper()
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	Expect(node.Labels).To(HaveKeyWithValue(upgrade.UpgradeStateLabelKey, "upgrade-required"),
		"expected node %s to keep its upgrade state label", nodeName)
}

// markClusterPolicyState settles the status the policy controller would
// normally decide, so the upgrade reconciler's status guard passes.
func markClusterPolicyState(ctx context.Context, nn types.NamespacedName, state string) {
	GinkgoHelper()
	policy := &rblnv1beta1.RBLNClusterPolicy{}
	Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
	policy.Status.State = state
	policy.Status.ObservedGeneration = policy.Generation
	Expect(k8sClient.Status().Update(ctx, policy)).To(Succeed())
}

func newUpgradeClusterPolicyFixture(name string, upgradePolicy *rblnv1beta1.DriverUpgradePolicySpec) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: "container",
			Driver: rblnv1beta1.DriverSpec{
				UpgradePolicy: upgradePolicy,
			},
			DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{Enabled: false},
			NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{Enabled: false},
			MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{Enabled: false},
			VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: false},
			SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
		},
	}
}

// restoreNodeSchedulable undoes a test's cordon. It tolerates a missing node:
// Ginkgo runs a spec's DeferCleanup after the container's AfterAll, which has
// already deleted the shared node when the spec is the last one to run.
func restoreNodeSchedulable(ctx context.Context, nodeName string) {
	var node corev1.Node
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		Expect(err).NotTo(HaveOccurred())
	}
	if !node.Spec.Unschedulable {
		return
	}
	node.Spec.Unschedulable = false
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())
}

func setNodeUnschedulable(ctx context.Context, nodeName string, unschedulable bool) {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	node.Spec.Unschedulable = unschedulable
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())
}

func expectNodeUnschedulable(ctx context.Context, nodeName string, want bool) {
	Eventually(func() bool {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		return node.Spec.Unschedulable
	}, 5*time.Second, 250*time.Millisecond).Should(Equal(want),
		"expected node %s unschedulable=%v", nodeName, want)
}

func setNodeAnnotation(ctx context.Context, nodeName, key, value string) {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[key] = value
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())
}

func removeNodeAnnotation(ctx context.Context, nodeName, key string) {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	if _, exists := node.Annotations[key]; !exists {
		return
	}
	delete(node.Annotations, key)
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())
}

func expectNodeLabelValue(ctx context.Context, nodeName, key, want string) {
	GinkgoHelper()
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	Expect(node.Labels).To(HaveKeyWithValue(key, want),
		"expected node %s to keep label %s=%s", nodeName, key, want)
}

func expectNodeKeepsAnnotation(ctx context.Context, nodeName, key string) {
	GinkgoHelper()
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	Expect(node.Annotations).To(HaveKey(key), "expected node %s to keep annotation %s", nodeName, key)
}

func expectNodeHasNoAnnotation(ctx context.Context, nodeName, key string) {
	Eventually(func() bool {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		_, exists := node.Annotations[key]
		return exists
	}, 5*time.Second, 250*time.Millisecond).Should(BeFalse(),
		"expected node %s annotation %s to be removed", nodeName, key)
}
