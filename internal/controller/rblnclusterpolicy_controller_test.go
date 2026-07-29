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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

var _ = Describe("RBLNClusterPolicy Controller", Ordered, func() {
	var (
		ctx        context.Context
		nodeName   string
		reconciler *RBLNClusterPolicyReconciler
		nn         types.NamespacedName
	)

	BeforeAll(func() {
		ctx = context.Background()
		nodeName = fmt.Sprintf("worker-%d", GinkgoParallelProcess())

		By("creating test node")
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					consts.NFDDevicePCILabelKey: "true",
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

	Context("When reconciling resources of container type", func() {
		var containerNS string

		BeforeEach(func() {
			containerNS = createTestNamespace(ctx, "rbln-container")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", containerNS)
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("container-policy"))
			DeferCleanup(func() { cleanupValidatorClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the cluster policy")
			reconcileClusterPolicy(ctx, reconciler, nn)
		})

		It("creates container workload resources and excludes vm-passthrough resources", func() {
			By("creating the device plugin resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)

			By("creating the NPU feature discovery resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-npu-feature-discovery", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-npu-feature-discovery", containerNS, 5*time.Second)

			By("creating the metrics exporter resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-metrics-exporter", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-metrics-exporter", containerNS, 5*time.Second)
			expectResource(ctx, &corev1.Service{}, "rbln-metrics-exporter-service", containerNS, 5*time.Second)

			By("not creating vm-passthrough resources")
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-vfio-manager", containerNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-sandbox-device-plugin", containerNS, 5*time.Second)

			By("marking the policy not ready until pods are running")
			expectReadyCondition(ctx, nn, consts.RBLNConditionReasonWorkloadProgressing)
		})

		It("Should clean up DevicePlugin artifacts when the component is disabled", func() {
			// first reconcile leaves the device-plugin resources in place
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)

			By("disabling the device plugin in the cluster policy")
			patchDevicePluginEnabled(ctx, nn, false)

			By("reconciling again after disabling the component")
			reconcileClusterPolicy(ctx, reconciler, nn)

			// artifacts should be gone once CleanUp runs
			expectResourceDeleted(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)
		})
	})

	Context("When reconciling resources of vm-passthrough type", func() {
		var vmNS string

		BeforeEach(func() {
			vmNS = createTestNamespace(ctx, "rbln-vm")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", vmNS)
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newVMPassthroughClusterPolicyFixture("container-policy"))
			DeferCleanup(func() { cleanupValidatorClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the cluster policy")
			reconcileClusterPolicy(ctx, reconciler, nn)
		})

		It("creates vm-passthrough resources and excludes container workload resources", func() {
			By("creating the VFIO manager resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-vfio-manager", vmNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-vfio-manager", vmNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-vfio-manager-config", vmNS, 5*time.Second)

			By("creating the sandbox device plugin resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-sandbox-device-plugin", vmNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-sandbox-device-plugin", vmNS, 5*time.Second)
			// alias-only contract: the legacy <name>-config ConfigMap is no
			// longer created. Patch idempotently deletes any stale instance.
			expectResourceDeleted(ctx, &corev1.ConfigMap{}, "rbln-sandbox-device-plugin-config", vmNS, 5*time.Second)

			By("not creating container workload resources")
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", vmNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-npu-feature-discovery", vmNS, 5*time.Second)

			By("marking the policy not ready until pods are running")
			expectReadyCondition(ctx, nn, consts.RBLNConditionReasonWorkloadProgressing)
		})
	})

	Context("When reconciling resources for skipped nodes", func() {
		var skipNS string

		BeforeEach(func() {
			skipNS = createTestNamespace(ctx, "rbln-operator-skip")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", skipNS)
			reconciler = newTestClusterPolicyReconciler("")
			setNodeLabel(ctx, nodeName, consts.RBLNDeploySkipLabelKey, "true")
			DeferCleanup(func() {
				removeNodeLabel(ctx, nodeName, consts.RBLNDeploySkipLabelKey)
			})
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("skip-policy"))
		})

		It("Should remove deploy labels and skip component deployment", func() {
			By("leaving the cluster policy not ready without requeue")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{}))

			By("marking the cluster policy as not ready (no deployable nodes)")
			expectReadyCondition(ctx, nn, consts.RBLNConditionReasonNoRBLNNodes)

			By("not creating component resources for the skipped node")
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", skipNS, 5*time.Second)

			By("keeping the node marked present")
			expectNodeLabel(ctx, nodeName, consts.RBLNPresentLabelKey, "true", 5*time.Second)

			By("removing deploy labels from the skipped node")
			expectNodeHasNoLabel(ctx, nodeName, "rebellions.ai/npu.deploy.device-plugin")
			expectNodeHasNoLabel(ctx, nodeName, "rebellions.ai/npu.deploy.driver")
		})
	})

	Context("When no NFD labels are present", func() {
		var nfdNS string

		BeforeEach(func() {
			nfdNS = createTestNamespace(ctx, "rbln-nfd-missing")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", nfdNS)
			reconciler = newTestClusterPolicyReconciler("")

			originalLabels := replaceNodeLabels(ctx, nodeName, map[string]string{
				"node-role.kubernetes.io/worker": "",
			})
			DeferCleanup(func() {
				replaceNodeLabels(ctx, nodeName, originalLabels)
			})

			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("nfd-missing-policy"))
		})

		It("returns a requeue result and marks the policy not ready", func() {
			By("reconciling the cluster policy")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, nn)

			By("requesting a retry after 30 seconds")
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))

			By("marking the cluster policy as not ready (NFD not found)")
			expectReadyCondition(ctx, nn, consts.RBLNConditionReasonNFDNotFound)
		})

		It("emits no contract events for this non-goal condition", func() {
			recorder := record.NewFakeRecorder(8)
			reconciler.Recorder = recorder
			reconcileClusterPolicyWithResult(ctx, reconciler, nn)
			expectNoPolicyEvent(recorder)
		})
	})

	Context("When the policy transitions to Ready", func() {
		var readyNS string

		BeforeEach(func() {
			readyNS = createTestNamespace(ctx, "rbln-ready-event")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", readyNS)
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("ready-event-policy"))
		})

		It("emits AllComponentsReady once on transition and not on repeats", func() {
			recorder := record.NewFakeRecorder(8)
			reconciler.Recorder = recorder

			policy := &rblnv1beta1.RBLNClusterPolicy{}
			Expect(k8sClient.Get(ctx, nn, policy)).To(Succeed())
			readyWorkloads := []rblnv1beta1.RBLNWorkloadStatus{{
				Type:  "container",
				State: rblnv1beta1.WorkloadStateReady,
			}}

			By("first Ready transition emits exactly one event")
			allReady, err := reconciler.reconcileStatus(ctx, policy, readyNS, nil, readyWorkloads)
			Expect(err).NotTo(HaveOccurred())
			Expect(allReady).To(BeTrue())
			expectPolicyEvent(recorder, corev1.EventTypeNormal, consts.RBLNConditionReasonAllComponentsReady)

			By("repeated Ready reconcile emits nothing")
			_, err = reconciler.reconcileStatus(ctx, policy, readyNS, nil, readyWorkloads)
			Expect(err).NotTo(HaveOccurred())
			expectNoPolicyEvent(recorder)
		})
	})

	Context("When reconciling multiple cluster policies", func() {
		var activeNS string
		var ignoredNS string
		var activeNN types.NamespacedName
		var ignoredNN types.NamespacedName

		BeforeEach(func() {
			activeNS = createTestNamespace(ctx, "rbln-singleton-active")
			ignoredNS = createTestNamespace(ctx, "rbln-singleton-ignored")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", activeNS)
			reconciler = newTestClusterPolicyReconciler("")
			activeNN = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("active-policy"))
			ignoredNN = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("ignored-policy"))
			DeferCleanup(func() { cleanupValidatorClusterRBAC(ctx) })
		})

		It("marks the second policy ignored and keeps resources scoped to the first policy", func() {
			By("reconciling the first cluster policy")
			reconcileClusterPolicy(ctx, reconciler, activeNN)

			By("reconciling the second cluster policy")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, ignoredNN)

			By("requeueing periodically as the promotion fallback")
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: singletonRecheckInterval}))

			By("marking the second policy as ignored")
			expectReadyCondition(ctx, ignoredNN, consts.RBLNConditionReasonPolicyIgnored)

			By("keeping resources only in the active policy namespace")
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", activeNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", ignoredNS, 5*time.Second)
		})

		It("emits PolicyIgnored once on transition and stays silent on repeats", func() {
			recorder := record.NewFakeRecorder(8)
			reconciler.Recorder = recorder

			reconcileClusterPolicy(ctx, reconciler, activeNN)
			drainPolicyEvents(recorder)

			By("first ignored reconcile emits exactly one PolicyIgnored")
			reconcileClusterPolicyWithResult(ctx, reconciler, ignoredNN)
			expectPolicyEvent(recorder, corev1.EventTypeNormal, consts.RBLNConditionReasonPolicyIgnored)

			By("repeated ignored reconcile emits nothing")
			reconcileClusterPolicyWithResult(ctx, reconciler, ignoredNN)
			expectNoPolicyEvent(recorder)
		})
	})

	Context("When cluster-scoped resources are created", func() {
		var ownerRefNS string

		BeforeEach(func() {
			ownerRefNS = createTestNamespace(ctx, "rbln-ownerref")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", ownerRefNS)
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("ownerref-policy"))
		})

		It("should set ownerReferences on cluster-scoped resources for GC", func() {
			By("reconciling to create resources")
			reconcileClusterPolicy(ctx, reconciler, nn)

			By("verifying the validator ClusterRole has an ownerReference to the policy")
			var cr rbacv1.ClusterRole
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rbln-operator-validator"}, &cr)).To(Succeed())
			Expect(cr.OwnerReferences).To(HaveLen(1))
			Expect(cr.OwnerReferences[0].Name).To(Equal(nn.Name))
			Expect(cr.OwnerReferences[0].Kind).To(Equal("RBLNClusterPolicy"))

			By("verifying the validator ClusterRoleBinding has an ownerReference to the policy")
			var crb rbacv1.ClusterRoleBinding
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rbln-operator-validator"}, &crb)).To(Succeed())
			Expect(crb.OwnerReferences).To(HaveLen(1))
			Expect(crb.OwnerReferences[0].Name).To(Equal(nn.Name))

			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, &cr)
				_ = k8sClient.Delete(ctx, &crb)
			})
		})
	})

	Context("When running on OpenShift: verify OpenShift-specific resources (RBAC/SCC)", func() {
		var openShiftNS string

		BeforeEach(func() {
			openShiftNS = createTestNamespace(ctx, "rbln-openshift")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", openShiftNS)
			reconciler = newTestClusterPolicyReconciler("v4.14.0")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("container-policy"))
			DeferCleanup(func() { cleanupValidatorClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the cluster policy")
			reconcileClusterPolicy(ctx, reconciler, nn)
		})

		It("creates OpenShift-specific RBAC resources for enabled components", func() {
			By("creating the OpenShift-specific RBAC for the device plugin")
			expectResource(ctx, &rbacv1.Role{}, "rbln-device-plugin", openShiftNS, 5*time.Second)
			expectResource(ctx, &rbacv1.RoleBinding{}, "rbln-device-plugin", openShiftNS, 5*time.Second)

			By("creating the OpenShift-specific RBAC for NPU feature discovery")
			expectResource(ctx, &rbacv1.Role{}, "rbln-npu-feature-discovery", openShiftNS, 5*time.Second)
			expectResource(ctx, &rbacv1.RoleBinding{}, "rbln-npu-feature-discovery", openShiftNS, 5*time.Second)
		})
	})
})

// ---------------------------------------------------------------------------
// Reconciler factory
// ---------------------------------------------------------------------------

// drainPolicyEvents empties the recorder so a spec only sees its own events.
func drainPolicyEvents(rec *record.FakeRecorder) {
	for {
		select {
		case <-rec.Events:
		default:
			return
		}
	}
}

func expectPolicyEvent(rec *record.FakeRecorder, eventType, reason string) {
	GinkgoHelper()
	select {
	case raw := <-rec.Events:
		Expect(raw).To(HavePrefix(eventType + " " + reason))
	case <-time.After(2 * time.Second):
		Fail("timed out waiting for event " + reason)
	}
}

func expectNoPolicyEvent(rec *record.FakeRecorder) {
	GinkgoHelper()
	select {
	case raw := <-rec.Events:
		Fail("unexpected event: " + raw)
	case <-time.After(100 * time.Millisecond):
	}
}

func newTestClusterPolicyReconciler(openShiftVersion string) *RBLNClusterPolicyReconciler {
	return &RBLNClusterPolicyReconciler{
		Client: k8sClient,
		Log:    logf.Log,
		Scheme: k8sClient.Scheme(),
		ClusterInfo: &clusterinfo.Info{
			OpenShiftVersion: openShiftVersion,
		},
		Conditions: conditions.NewUpdater(k8sClient),
	}
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

func newContainerClusterPolicyFixture(name string) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: consts.RBLNWorkloadConfigContainer,
			VFIOManager: rblnv1beta1.RBLNVFIOManagerSpec{
				Enabled: false,
			},
			SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{
				Enabled: false,
			},
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
				Enabled: true,
			},
			NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{
				Enabled: true,
			},
			MetricsExporter: rblnv1beta1.RBLNMetricsExporterSpec{
				Enabled: true,
			},
		},
	}
}

func newVMPassthroughClusterPolicyFixture(name string) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: consts.RBLNWorkloadConfigVMPassthrough,
			VFIOManager: rblnv1beta1.RBLNVFIOManagerSpec{
				Enabled: true,
			},
			SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{
				Enabled: true,
			},
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
				Enabled: false,
			},
			NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{
				Enabled: false,
			},
			MetricsExporter: rblnv1beta1.RBLNMetricsExporterSpec{
				Enabled: false,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Kubernetes resource lifecycle helpers
// ---------------------------------------------------------------------------

func createTestNamespace(ctx context.Context, prefix string) string {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{GenerateName: prefix + "-"},
	}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	DeferCleanup(func() {
		_ = k8sClient.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns.Name},
		})
	})
	return ns.Name
}

func createClusterPolicyFixture(ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy) types.NamespacedName {
	Expect(k8sClient.Create(ctx, policy)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(ctx, policy) })
	return types.NamespacedName{Name: policy.Name, Namespace: policy.Namespace}
}

// ---------------------------------------------------------------------------
// Reconcile helpers
// ---------------------------------------------------------------------------

func reconcileClusterPolicy(ctx context.Context, reconciler *RBLNClusterPolicyReconciler, nn types.NamespacedName) {
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
	Expect(err).NotTo(HaveOccurred())
}

func reconcileClusterPolicyWithResult(
	ctx context.Context,
	reconciler *RBLNClusterPolicyReconciler,
	nn types.NamespacedName,
) ctrl.Result {
	result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
	Expect(err).NotTo(HaveOccurred())
	return result
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func expectResource[T client.Object](ctx context.Context, obj T, name, ns string, timeout time.Duration) {
	Eventually(func() error {
		return k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, obj)
	}, timeout, 250*time.Millisecond).Should(Succeed(), "expected %T %s/%s to exist", obj, ns, name)
}

func expectResourceDeleted[T client.Object](ctx context.Context, obj T, name, ns string, timeout time.Duration) {
	Eventually(func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, obj)
		return apierrors.IsNotFound(err)
	}, timeout, 250*time.Millisecond).Should(BeTrue(), "expected %T %s/%s to be removed", obj, ns, name)
}

func expectReadyCondition(
	ctx context.Context,
	nn types.NamespacedName,
	reason string,
) {
	Eventually(func() bool {
		var policy rblnv1beta1.RBLNClusterPolicy
		Expect(k8sClient.Get(ctx, nn, &policy)).To(Succeed())
		for _, c := range policy.Status.Conditions {
			if c.Type == consts.RBLNConditionTypeReady && c.Status == metav1.ConditionFalse && c.Reason == reason {
				return true
			}
		}
		return false
	}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(), "expected Ready condition status=%s reason=%s", metav1.ConditionFalse, reason)
}

func expectNodeLabel(ctx context.Context, nodeName, key, want string, timeout time.Duration) {
	Eventually(func() string {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		return node.Labels[key]
	}, timeout, 250*time.Millisecond).Should(Equal(want), "expected node %s label %s=%s", nodeName, key, want)
}

func expectNodeHasNoLabel(ctx context.Context, nodeName, key string) {
	Eventually(func() bool {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		_, exists := node.Labels[key]
		return exists
	}, 5*time.Second, 250*time.Millisecond).Should(BeFalse(), "expected node %s label %s to be removed", nodeName, key)
}

// ---------------------------------------------------------------------------
// Node manipulation helpers
// ---------------------------------------------------------------------------

func setNodeLabel(ctx context.Context, nodeName, key, value string) {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[key] = value
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())
}

func removeNodeLabel(ctx context.Context, nodeName, key string) {
	var node corev1.Node
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return
	}
	delete(node.Labels, key)
	_ = k8sClient.Update(ctx, &node)
}

func replaceNodeLabels(ctx context.Context, nodeName string, labels map[string]string) map[string]string {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())

	previous := cloneLabels(node.Labels)
	node.Labels = cloneLabels(labels)
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())

	return previous
}

func patchDevicePluginEnabled(ctx context.Context, nn types.NamespacedName, enabled bool) {
	var policy rblnv1beta1.RBLNClusterPolicy
	Expect(k8sClient.Get(ctx, nn, &policy)).To(Succeed())

	patch := []byte(fmt.Sprintf(`[{"op": "replace", "path": "/spec/devicePlugin/enabled", "value": %t}]`, enabled))
	Expect(k8sClient.Patch(ctx, &policy, client.RawPatch(types.JSONPatchType, patch))).To(Succeed())
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

func cleanupValidatorClusterRBAC(ctx context.Context) {
	_ = k8sClient.Delete(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "rbln-operator-validator"}})
	_ = k8sClient.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "rbln-operator-validator"}})
}

func cloneLabels(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
