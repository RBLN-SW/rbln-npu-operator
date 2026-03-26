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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
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
					"feature.node.kubernetes.io/pci-1eff.present": "true",
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
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("container-policy", containerNS))
		})

		JustBeforeEach(func() {
			By("reconciling the cluster policy")
			reconcileClusterPolicy(ctx, reconciler, nn)
		})

		It("creates container workload resources and excludes vm-passthrough resources", func() {
			By("creating the device plugin resources")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", containerNS, 5*time.Second)

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
		})

		It("Should clean up DevicePlugin artifacts when the component is disabled", func() {
			// first reconcile leaves the device-plugin resources in place
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", containerNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)

			By("disabling the device plugin in the cluster policy")
			patchDevicePluginEnabled(ctx, nn, false)

			By("reconciling again after disabling the component")
			reconcileClusterPolicy(ctx, reconciler, nn)

			// artifacts should be gone once CleanUp runs
			expectResourceDeleted(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", containerNS, 5*time.Second)
			expectResourceDeleted(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", containerNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", containerNS, 5*time.Second)
		})
	})

	Context("When reconciling resources of vm-passthrough type", func() {
		var vmNS string

		BeforeEach(func() {
			vmNS = createTestNamespace(ctx, "rbln-vm")
			reconciler = newTestClusterPolicyReconciler("")
			nn = createClusterPolicyFixture(ctx, newVMPassthroughClusterPolicyFixture("container-policy", vmNS))
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
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-sandbox-device-plugin-config", vmNS, 5*time.Second)

			By("not creating container workload resources")
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", vmNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-npu-feature-discovery", vmNS, 5*time.Second)
		})
	})

	Context("When reconciling resources for skipped nodes", func() {
		var skipNS string

		BeforeEach(func() {
			skipNS = createTestNamespace(ctx, "rbln-operator-skip")
			reconciler = newTestClusterPolicyReconciler("")
			setNodeLabel(ctx, nodeName, consts.RBLNDeploySkipLabelKey, "true")
			DeferCleanup(func() {
				removeNodeLabel(ctx, nodeName, consts.RBLNDeploySkipLabelKey)
			})
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("skip-policy", skipNS))
		})

		It("Should remove deploy labels and skip component deployment", func() {
			By("leaving the cluster policy not ready without requeue")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{}))

			By("marking the cluster policy as not ready")
			expectClusterPolicyState(ctx, nn, rblnv1beta1.ClusterNotReady, 5*time.Second)

			By("not creating component resources for the skipped node")
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", skipNS, 5*time.Second)

			By("keeping the node marked present")
			expectNodeLabel(ctx, nodeName, consts.RBLNPresentLabelKey, "true", 5*time.Second)

			By("removing deploy labels from the skipped node")
			expectNodeHasNoLabel(ctx, nodeName, "rebellions.ai/npu.deploy.device-plugin", 5*time.Second)
			expectNodeHasNoLabel(ctx, nodeName, "rebellions.ai/npu.deploy.driver", 5*time.Second)
		})
	})

	Context("When no NFD labels are present", func() {
		var nfdNS string

		BeforeEach(func() {
			nfdNS = createTestNamespace(ctx, "rbln-nfd-missing")
			reconciler = newTestClusterPolicyReconciler("")

			originalLabels := replaceNodeLabels(ctx, nodeName, map[string]string{
				"node-role.kubernetes.io/worker": "",
			})
			DeferCleanup(func() {
				replaceNodeLabels(ctx, nodeName, originalLabels)
			})

			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("nfd-missing-policy", nfdNS))
		})

		It("returns a requeue result and marks the policy not ready", func() {
			By("reconciling the cluster policy")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, nn)

			By("requesting a retry after 30 seconds")
			Expect(result).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))

			By("marking the cluster policy as not ready")
			expectClusterPolicyState(ctx, nn, rblnv1beta1.ClusterNotReady, 5*time.Second)
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
			reconciler = newTestClusterPolicyReconciler("")
			activeNN = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("active-policy", activeNS))
			ignoredNN = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("ignored-policy", ignoredNS))
		})

		It("marks the second policy ignored and keeps resources scoped to the first policy", func() {
			By("reconciling the first cluster policy")
			reconcileClusterPolicy(ctx, reconciler, activeNN)

			By("reconciling the second cluster policy")
			result := reconcileClusterPolicyWithResult(ctx, reconciler, ignoredNN)

			By("returning without requeue")
			Expect(result).To(Equal(ctrl.Result{}))

			By("marking the second policy as ignored")
			expectClusterPolicyState(ctx, ignoredNN, rblnv1beta1.ClusterIgnored, 5*time.Second)

			By("keeping resources only in the active policy namespace")
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", activeNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", ignoredNS, 5*time.Second)
		})
	})

	Context("When running on OpenShift: verify OpenShift-specific resources (RBAC/SCC)", func() {
		var openShiftNS string

		BeforeEach(func() {
			openShiftNS = createTestNamespace(ctx, "rbln-openshift")
			reconciler = newTestClusterPolicyReconciler("v4.14.0")
			nn = createClusterPolicyFixture(ctx, newContainerClusterPolicyFixture("container-policy", openShiftNS))
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

func newTestClusterPolicyReconciler(openShiftVersion string) *RBLNClusterPolicyReconciler {
	return &RBLNClusterPolicyReconciler{
		Client: k8sClient,
		Scheme: k8sClient.Scheme(),
		ClusterInfo: &clusterinfo.Info{
			OpenShiftVersion: openShiftVersion,
		},
	}
}

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

func newContainerClusterPolicyFixture(name, namespace string) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: "container",
			Namespace:    namespace,
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

func newVMPassthroughClusterPolicyFixture(name, namespace string) *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: "vm-passthrough",
			Namespace:    namespace,
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

func patchDevicePluginEnabled(ctx context.Context, nn types.NamespacedName, enabled bool) {
	var policy rblnv1beta1.RBLNClusterPolicy
	Expect(k8sClient.Get(ctx, nn, &policy)).To(Succeed())

	patch := []byte(fmt.Sprintf(`[{"op": "replace", "path": "/spec/devicePlugin/enabled", "value": %t}]`, enabled))
	Expect(k8sClient.Patch(ctx, &policy, client.RawPatch(types.JSONPatchType, patch))).To(Succeed())
}

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

func expectNodeLabel(ctx context.Context, nodeName, key, want string, timeout time.Duration) {
	Eventually(func() string {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		return node.Labels[key]
	}, timeout, 250*time.Millisecond).Should(Equal(want), "expected node %s label %s=%s", nodeName, key, want)
}

func expectNodeHasNoLabel(ctx context.Context, nodeName, key string, timeout time.Duration) {
	Eventually(func() bool {
		var node corev1.Node
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())
		_, exists := node.Labels[key]
		return exists
	}, timeout, 250*time.Millisecond).Should(BeFalse(), "expected node %s label %s to be removed", nodeName, key)
}

func expectClusterPolicyState(
	ctx context.Context,
	nn types.NamespacedName,
	want rblnv1beta1.ClusterState,
	timeout time.Duration,
) {
	Eventually(func() rblnv1beta1.ClusterState {
		var policy rblnv1beta1.RBLNClusterPolicy
		Expect(k8sClient.Get(ctx, nn, &policy)).To(Succeed())
		return policy.Status.State
	}, timeout, 250*time.Millisecond).Should(Equal(want))
}

func replaceNodeLabels(ctx context.Context, nodeName string, labels map[string]string) map[string]string {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)).To(Succeed())

	previous := cloneLabels(node.Labels)
	node.Labels = cloneLabels(labels)
	Expect(k8sClient.Update(ctx, &node)).To(Succeed())

	return previous
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
