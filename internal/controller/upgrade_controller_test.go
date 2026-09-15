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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
	applyStateFunc func(ctx context.Context, namespace string, currentState *upgrade.ClusterUpgradeState, upgradePolicy *rblnv1beta1.DriverUpgradePolicySpec) error
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

func (m *mockStateManager) ApplyState(ctx context.Context, namespace string, currentState *upgrade.ClusterUpgradeState, upgradePolicy *rblnv1beta1.DriverUpgradePolicySpec) error {
	if m.applyStateFunc != nil {
		return m.applyStateFunc(ctx, namespace, currentState, upgradePolicy)
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
				applyStateFunc: func(_ context.Context, _ string, _ *upgrade.ClusterUpgradeState, _ *rblnv1beta1.DriverUpgradePolicySpec) error {
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
		Scheme:       k8sClient.Scheme(),
		Namespace:    "test-namespace",
		StateManager: sm,
	}
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
