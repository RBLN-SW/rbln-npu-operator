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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
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
					"rebellions.ai/npu.present":                               "true",
					"rebellions.ai/npu.deploy.driver":                         "true",
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
			nn = createDriverFixture(ctx, newDriverFixture("test-driver"))
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		JustBeforeEach(func() {
			By("reconciling the driver")
			reconcileDriver(ctx, reconciler, nn)
		})

		It("creates driver manager resources", func() {
			By("creating the driver ServiceAccount")
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-driver", driverNS, 5*time.Second)

			By("creating the driver ClusterRole")
			expectClusterResource(ctx, &rbacv1.ClusterRole{}, "rbln-driver", 5*time.Second)

			By("creating the driver ClusterRoleBinding")
			expectClusterResource(ctx, &rbacv1.ClusterRoleBinding{}, "rbln-driver", 5*time.Second)

			By("creating the startup probe ConfigMap")
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-driver-startup-probe", driverNS, 5*time.Second)

			By("creating a DaemonSet for the detected node pool")
			expectResource(ctx, &appsv1.DaemonSet{}, "test-driver-ubuntu22.04-5.15.0-100-generic", driverNS, 5*time.Second)
		})
	})

	Context("When no RBLNClusterPolicy exists", func() {
		var (
			reconciler *RBLNDriverReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			reconciler = newTestDriverReconciler("")
			nn = createDriverFixture(ctx, newDriverFixture("orphan-driver"))
		})

		It("sets status to not ready with MissingClusterPolicy reason", func() {
			result := reconcileDriverWithResult(ctx, reconciler, nn)
			Expect(result).To(Equal(ctrl.Result{}))
			expectDriverReadyCondition(ctx, nn, consts.RBLNConditionReasonMissingClusterPolicy)
		})
	})

	Context("When nodeSelector overlaps with another driver", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			nn2        types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-overlap")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("overlap-cp"))

			By("creating and reconciling the first driver")
			nn1 := createDriverFixture(ctx, newDriverFixture("driver-a"))
			reconcileDriver(ctx, reconciler, nn1)

			By("creating the second driver with the same default selector")
			nn2 = createDriverFixture(ctx, newDriverFixture("driver-b"))
			DeferCleanup(func() { cleanupDriverClusterRBAC(ctx) })
		})

		It("sets error status on the second driver due to selector conflict", func() {
			reconcileDriverWithResult(ctx, reconciler, nn2)
			expectDriverErrorCondition(ctx, nn2, consts.RBLNConditionReasonReconcileFailed)
		})
	})

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

	Context("When the driver resource is deleted", func() {
		var (
			driverNS   string
			reconciler *RBLNDriverReconciler
			nn         types.NamespacedName
		)

		BeforeEach(func() {
			driverNS = createTestNamespace(ctx, "rbln-driver-del")
			GinkgoT().Setenv("OPERATOR_NAMESPACE", driverNS)
			reconciler = newTestDriverReconciler("")
			createClusterPolicyFixture(ctx, newDriverTestClusterPolicy("del-cp"))
			nn = createDriverFixture(ctx, newDriverFixture("del-driver"))
		})

		It("should add a finalizer and clean up cluster-scoped resources on deletion", func() {
			By("reconciling to add finalizer and create resources")
			reconcileDriver(ctx, reconciler, nn)

			By("verifying the finalizer is present")
			var driver rebellionsaiv1alpha1.RBLNDriver
			Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
			Expect(driver.Finalizers).To(ContainElement(consts.DriverFinalizer))

			By("verifying cluster-scoped resources exist")
			expectClusterResource(ctx, &rbacv1.ClusterRole{}, "rbln-driver", 5*time.Second)
			expectClusterResource(ctx, &rbacv1.ClusterRoleBinding{}, "rbln-driver", 5*time.Second)

			By("deleting the driver")
			Expect(k8sClient.Delete(ctx, &driver)).To(Succeed())

			By("reconciling the deletion")
			reconcileDriver(ctx, reconciler, nn)

			By("verifying cluster-scoped resources are cleaned up")
			expectClusterResourceDeleted(ctx, &rbacv1.ClusterRole{}, "rbln-driver", 5*time.Second)
			expectClusterResourceDeleted(ctx, &rbacv1.ClusterRoleBinding{}, "rbln-driver", 5*time.Second)

			By("verifying the CR is fully deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, nn, &rebellionsaiv1alpha1.RBLNDriver{})
				return apierrors.IsNotFound(err)
			}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(), "expected RBLNDriver to be fully deleted")
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
})

// ---------------------------------------------------------------------------
// Reconciler factory
// ---------------------------------------------------------------------------

func newTestDriverReconciler(openShiftVersion string) *RBLNDriverReconciler {
	r := &RBLNDriverReconciler{
		Client: k8sClient,
		Log:    logf.Log,
		Scheme: k8sClient.Scheme(),
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
	DeferCleanup(func() {
		var d rebellionsaiv1alpha1.RBLNDriver
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(driver), &d); err == nil {
			if len(d.Finalizers) > 0 {
				d.Finalizers = nil
				_ = k8sClient.Update(ctx, &d)
			}
		}
		_ = k8sClient.Delete(ctx, driver)
	})
	return types.NamespacedName{Name: driver.Name}
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

func expectDriverReadyCondition(ctx context.Context, nn types.NamespacedName, reason string) {
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

func expectDriverErrorCondition(ctx context.Context, nn types.NamespacedName, reason string) {
	Eventually(func() bool {
		var driver rebellionsaiv1alpha1.RBLNDriver
		Expect(k8sClient.Get(ctx, nn, &driver)).To(Succeed())
		for _, c := range driver.Status.Conditions {
			if c.Type == consts.RBLNConditionTypeError && c.Status == metav1.ConditionTrue && c.Reason == reason {
				return true
			}
		}
		return false
	}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(),
		"expected Driver Error condition status=%s reason=%s", metav1.ConditionTrue, reason)
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
