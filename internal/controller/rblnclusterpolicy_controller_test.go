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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
)

var _ = Describe("RBLNClusterPolicy Controller", Ordered, func() {
	var (
		ctx        context.Context
		targetNS   string
		nodeName   string
		reconciler *RBLNClusterPolicyReconciler
		nn         types.NamespacedName
	)

	BeforeAll(func() {
		ctx = context.Background()
		targetNS = fmt.Sprintf("rbln-operator-system-%d", GinkgoParallelProcess())
		nodeName = fmt.Sprintf("worker-%d", GinkgoParallelProcess())

		By("creatring test namespace")
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targetNS}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		By("creatring test node")
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
		By("Deleting test namespace")
		ns := &corev1.Namespace{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: targetNS}, ns); err == nil {
			_ = k8sClient.Delete(ctx, ns)
		}

		By("Deleting test node")
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
		BeforeEach(func() {
			reconciler = &RBLNClusterPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				ClusterInfo: &clusterinfo.Info{
					OpenShiftVersion: "",
				},
			}

			cr := &rblnv1beta1.RBLNClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "container-policy",
					Namespace: "default",
				},
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					WorkloadType: "container",
					Namespace:    targetNS,
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
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })
			nn = types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
		})

		JustBeforeEach(func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nn,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should reconcile DevicePlugin", func() {
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", targetNS, 5*time.Second)
		})

		It("Should reconcile NPU Feature Discovery", func() {
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-npu-feature-discovery", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-npu-feature-discovery", targetNS, 5*time.Second)
		})

		It("Should reconcile MetricsExporter", func() {
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-metrics-exporter", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-metrics-exporter", targetNS, 5*time.Second)
			expectResource(ctx, &corev1.Service{}, "rbln-metrics-exporter-service", targetNS, 5*time.Second)
		})

		It("Should clean up DevicePlugin artifacts when the component is disabled", func() {
			// first reconcile leaves the device-plugin resources in place
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", targetNS, 5*time.Second)

			Eventually(func(g Gomega) {
				// 매번 최신 버전을 가져와서 업데이트
				var policy rblnv1beta1.RBLNClusterPolicy
				g.Expect(k8sClient.Get(ctx, nn, &policy)).To(Succeed())

				patch := []byte(`[{"op": "replace", "path": "/spec/devicePlugin/enabled", "value": false}]`)
				err := k8sClient.Patch(ctx, &policy, client.RawPatch(types.JSONPatchType, patch))
				if err != nil {
					return
				}

				var check rblnv1beta1.RBLNClusterPolicy
				g.Expect(k8sClient.Get(ctx, nn, &check)).To(Succeed())
				g.Expect(check.Spec.DevicePlugin.Enabled).To(BeFalse())
			}, 15*time.Second, 1*time.Second).Should(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			// artifacts should be gone once CleanUp runs
			expectResourceDeleted(ctx, &corev1.ServiceAccount{}, "rbln-device-plugin", targetNS, 5*time.Second)
			expectResourceDeleted(ctx, &corev1.ConfigMap{}, "rbln-device-plugin-config", targetNS, 5*time.Second)
			expectResourceDeleted(ctx, &appsv1.DaemonSet{}, "rbln-device-plugin", targetNS, 5*time.Second)
		})
	})

	Context("When reconciling resources of vm-passthrough type", func() {
		BeforeEach(func() {
			reconciler = &RBLNClusterPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				ClusterInfo: &clusterinfo.Info{
					OpenShiftVersion: "",
				},
			}

			cr := &rblnv1beta1.RBLNClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "container-policy",
					Namespace: "default",
				},
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					WorkloadType: "vm-passthrough",
					Namespace:    targetNS,
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
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })
			nn = types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
		})

		JustBeforeEach(func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nn,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should reconcile VFIOManager", func() {
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-vfio-manager", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-vfio-manager", targetNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-vfio-manager-config", targetNS, 5*time.Second)
		})

		It("Should reconcile Sandbox device plugin", func() {
			expectResource(ctx, &corev1.ServiceAccount{}, "rbln-sandbox-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &appsv1.DaemonSet{}, "rbln-sandbox-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &corev1.ConfigMap{}, "rbln-sandbox-device-plugin-config", targetNS, 5*time.Second)
		})
	})

	Context("When running on OpenShift: verify OpenShift-specific resources (RBAC/SCC)", func() {
		BeforeEach(func() {
			reconciler = &RBLNClusterPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				ClusterInfo: &clusterinfo.Info{
					OpenShiftVersion: "v4.14.0",
				},
			}

			cr := &rblnv1beta1.RBLNClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "container-policy",
					Namespace: "default",
				},
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					WorkloadType: "container",
					Namespace:    targetNS,
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
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })
			nn = types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
		})

		JustBeforeEach(func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nn,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should reconcile DevicePlugin", func() {
			expectResource(ctx, &rbacv1.Role{}, "rbln-device-plugin", targetNS, 5*time.Second)
			expectResource(ctx, &rbacv1.RoleBinding{}, "rbln-device-plugin", targetNS, 5*time.Second)
		})

		It("Should reconcile NPU Feature Discovery", func() {
			expectResource(ctx, &rbacv1.Role{}, "rbln-npu-feature-discovery", targetNS, 5*time.Second)
			expectResource(ctx, &rbacv1.RoleBinding{}, "rbln-npu-feature-discovery", targetNS, 5*time.Second)
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
