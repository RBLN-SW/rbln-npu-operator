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

package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	e2ek8s "github.com/rebellions-sw/rbln-npu-operator/test/e2e/kubernetes"
	e2elog "github.com/rebellions-sw/rbln-npu-operator/test/e2e/logs"
	"github.com/rebellions-sw/rbln-npu-operator/test/e2e/testenv"
)

const (
	// per-model contract: the operator injects an empty RBLN_PT_ALIAS, so
	// the sandbox-device-plugin advertises one resource per (model, function)
	// derived from sysfs+pci.ids — e.g. "rebellions.ai/RBLN-CA22_PF".
	// The value below targets the current sandbox cluster's NPU; override
	// when running against a node with different hardware.
	vmpAtomResourceName       = corev1.ResourceName("rebellions.ai/RBLN-CA22_PF")
	vmpSandboxNodeLabelKey    = "rebellions.ai/npu.deploy.sandbox-device-plugin"
	vmpSandboxNodeLabelValue  = "true"
	vmpName                   = "rbln-npu-vmtest"
	vmpVirtLauncherLabelKey   = "kubevirt.io"
	vmpVirtLauncherLabelValue = "virt-launcher"
	vmpVMStartTimeout         = 5 * time.Minute
	vmpVMRunningTimeout       = 10 * time.Minute
	vmpInGuestCheckTimeout    = 5 * time.Minute
	vmpContainerDiskImage     = "quay.io/containerdisks/ubuntu:22.04"
	vmpKubeVirtAPIVersion     = "kubevirt.io/v1"
	vmpVirtualMachineKind     = "VirtualMachine"
)

var (
	vmGVR = schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachines",
	}
	vmiGVR = schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachineinstances",
	}
)

const vmpCloudInitUserData = `#cloud-config
runcmd:
  - |
    lspci -d 1eff: 2>/dev/null > /var/log/npu-check.log
    if [ -s /var/log/npu-check.log ]; then
      touch /tmp/.npu-found
    fi
power_state:
  mode: poweroff
  delay: now
  condition: 'test -e /tmp/.npu-found'
`

var _ = Describe("e2e-npu-operator-vm-passthrough", Ordered, Label("vm-passthrough"), func() {
	te := testenv.NewTestEnv()

	Describe("NPU Operator RBLNClusterPolicy (vm-passthrough)", func() {
		Context("VM-passthrough NPU Operator deployment", Ordered, func() {
			/*
				Scenario:
				- Deploy NPU Operator with workloadType=vm-passthrough.
				- Verify sandbox-device-plugin and vfio-manager pods are Ready.
				- Verify a per-model NPU resource (vmpAtomResourceName, e.g.
				  rebellions.ai/RBLN-CA22_PF) is advertised on NPU nodes.
				- Apply a KubeVirt VirtualMachine with hostDevices referencing
				  that resource, then assert:
					- virt-launcher Pod requests the per-model resource
					- VMI reaches Phase: Running (KubeVirt-side VFIO attach OK)
					- VMI reaches Phase: Succeeded (cloud-init found the device
					  inside the guest via lspci -d 1eff: and triggered poweroff)
			*/

			var (
				helmClient      *HelmClient
				helmReleaseName string
				k8sCoreClient   *e2ek8s.CoreClient
				testNamespace   *corev1.Namespace
				setupSucceeded  bool
			)

			BeforeAll(func(ctx context.Context) {
				helmClient, helmReleaseName, k8sCoreClient, testNamespace = setupOperatorDeployment(
					ctx,
					te,
					"rbln-npu-operator",
					"VMPassthroughHelmReleaseName",
					buildSandboxOperatorHelmValues(),
					false,
				)
				setupSucceeded = true
			})

			AfterAll(func(ctx context.Context) {
				_ = te.DynamicClient.Resource(vmGVR).Namespace(e2eCfg.namespace).
					Delete(ctx, vmpName, metav1.DeleteOptions{})

				if setupSucceeded {
					if err := helmClient.Uninstall(ctx, helmReleaseName); err != nil {
						Expect(err).NotTo(HaveOccurred())
					}
				}

				k8sExtensionsClient := e2ek8s.NewExtensionClient(te.ExtClientSet)
				err := k8sExtensionsClient.DeleteCRD(ctx, rblnClusterPolicyCRDName)
				Expect(err).NotTo(HaveOccurred())
				err = k8sExtensionsClient.DeleteCRD(ctx, rblnDriverCRDName)
				Expect(err).NotTo(HaveOccurred())

				cleanupCoreClient := e2ek8s.NewClient(te.ClientSet.CoreV1())
				err = cleanupCoreClient.DeleteNamespace(ctx, e2eCfg.namespace)
				if err != nil && !kapierrors.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("should bring up sandbox-device-plugin and vfio-manager pods", func(ctx context.Context) {
				operands := []struct {
					name      string
					component string
				}{
					{"rbln-sandbox-device-plugin", "sandbox-device-plugin"},
					{"rbln-vfio-manager", "vfio-manager"},
				}
				e2elog.Infof("Ensure that the vm-passthrough operands come up")
				for _, operand := range operands {
					waitForPodsReady(ctx, k8sCoreClient, testNamespace.Name, operand.name, map[string]string{
						appComponentLabelKey: operand.component,
					})
				}
			})

			It("should advertise the per-model NPU resource on sandbox-device-plugin-labeled nodes", func(ctx context.Context) {
				Eventually(func(g Gomega) bool {
					nodes, err := k8sCoreClient.ListNodes(ctx, map[string]string{
						vmpSandboxNodeLabelKey: vmpSandboxNodeLabelValue,
					})
					g.Expect(err).NotTo(HaveOccurred())

					found := false
					for i := range nodes {
						node := &nodes[i]
						if !k8sCoreClient.IsNodeReady(node) {
							continue
						}

						allocQty, allocOK := node.Status.Allocatable[vmpAtomResourceName]
						if !allocOK || allocQty.Value() == 0 {
							e2elog.Infof("node %s has no allocatable %s", node.Name, vmpAtomResourceName)
							return false
						}

						capQty, capOK := node.Status.Capacity[vmpAtomResourceName]
						if !capOK || capQty.Value() == 0 {
							e2elog.Infof("node %s has no capacity %s", node.Name, vmpAtomResourceName)
							return false
						}

						found = true
					}
					return found
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "no ready labeled node exposed %s", vmpAtomResourceName)
			})

			It("should run a KubeVirt VM whose hostDevices reference the per-model NPU resource and reach Running phase", func(ctx context.Context) {
				vm := buildHostDeviceVM(vmpName, testNamespace.Name, vmpCloudInitUserData)
				_, err := te.DynamicClient.Resource(vmGVR).Namespace(testNamespace.Name).
					Create(ctx, vm, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())

				By(fmt.Sprintf("waiting for the virt-launcher Pod to be created and request %s", vmpAtomResourceName))
				Eventually(func(g Gomega) bool {
					if msg := vmiSyncFailure(ctx, te, testNamespace.Name, vmpName); msg != "" {
						StopTrying(fmt.Sprintf("VMI %s cannot be launched: %s", vmpName, msg)).Now()
					}
					pod, err := findVirtLauncherPod(ctx, k8sCoreClient, testNamespace.Name, vmpName)
					g.Expect(err).NotTo(HaveOccurred())
					if pod == nil {
						return false
					}
					compute := findContainer(pod.Spec.Containers, "compute")
					if compute == nil {
						e2elog.Infof("virt-launcher pod %s has no 'compute' container yet", pod.Name)
						return false
					}
					qty, ok := compute.Resources.Limits[vmpAtomResourceName]
					if !ok || qty.Value() == 0 {
						e2elog.Infof("virt-launcher pod %s does not request %s", pod.Name, vmpAtomResourceName)
						return false
					}
					e2elog.Infof("virt-launcher pod %s requests %s=%d", pod.Name, vmpAtomResourceName, qty.Value())
					return true
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(vmpVMStartTimeout).
					Should(BeTrue(), func() string {
						return fmt.Sprintf("virt-launcher pod did not request %s\n%s",
							vmpAtomResourceName, describeVMI(ctx, te, testNamespace.Name, vmpName))
					})

				By("waiting for the VMI to reach Phase: Running")
				Eventually(func(g Gomega) string {
					return getVMIPhase(ctx, g, te, testNamespace.Name, vmpName)
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(vmpVMRunningTimeout).
					Should(Equal("Running"), func() string {
						return fmt.Sprintf("VMI %s never reached Running\n%s",
							vmpName, describeVMI(ctx, te, testNamespace.Name, vmpName))
					})
			})

			It("should observe in-guest lspci hit and clean shutdown (VM Stopped)", func(ctx context.Context) {
				By("waiting for the VM to reach printableStatus: Stopped (cloud-init poweroff after lspci hit)")
				Eventually(func(g Gomega) string {
					return getVMPrintableStatus(ctx, g, te, testNamespace.Name, vmpName)
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(vmpInGuestCheckTimeout).
					Should(Equal("Stopped"),
						"VM %s did not reach Stopped state; in-guest lspci likely did not match 1eff:*",
						vmpName)
			})
		})
	})
})

// ---------------------------------------------------------------------------
// Helm values for vm-passthrough workload
// ---------------------------------------------------------------------------

func buildSandboxOperatorHelmValues() map[string]interface{} {
	return map[string]interface{}{
		"workloadType": "vm-passthrough",
		"operator": map[string]interface{}{
			"image": map[string]interface{}{
				"registry":   e2eCfg.operatorRegistry,
				"repository": e2eCfg.operatorRepository,
				"tag":        e2eCfg.operatorVersion,
			},
		},
		"driver":              map[string]interface{}{"enabled": false},
		"devicePlugin":        map[string]interface{}{"enabled": false},
		"draKubeletPlugin":    map[string]interface{}{"enabled": false},
		"metricsExporter":     map[string]interface{}{"enabled": false},
		"rblnDaemon":          map[string]interface{}{"enabled": false},
		"containerToolkit":    map[string]interface{}{"enabled": false},
		"npuFeatureDiscovery": map[string]interface{}{"enabled": false},
		// per-model: resourceList is omitted; the binary advertises
		// rebellions.ai/RBLN-<MODEL>_<PF|VF> from sysfs+pci.ids.
		"sandboxDevicePlugin": map[string]interface{}{
			"enabled": true,
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
		"vfioManager": map[string]interface{}{
			"enabled": true,
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
		"validator": map[string]interface{}{
			"image": map[string]interface{}{
				"registry":   e2eCfg.validatorRegistry,
				"repository": e2eCfg.validatorRepository,
				"tag":        e2eCfg.operatorVersion,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// KubeVirt VM builders / lookups (unstructured to avoid kubevirt.io/client-go)
// ---------------------------------------------------------------------------

func buildHostDeviceVM(name, namespace, userData string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": vmpKubeVirtAPIVersion,
			"kind":       vmpVirtualMachineKind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				// RerunOnFailure: a graceful poweroff (cloud-init `poweroff`
				// after lspci hit) stays stopped, while a crash restarts. The
				// e2e success signal is exactly this graceful poweroff.
				"runStrategy": "RerunOnFailure",
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"kubevirt.io/domain": name,
						},
					},
					"spec": map[string]interface{}{
						"domain": map[string]interface{}{
							"cpu": map[string]interface{}{
								"cores": int64(2),
							},
							"resources": map[string]interface{}{
								"requests": map[string]interface{}{
									"memory": "2Gi",
								},
								"limits": map[string]interface{}{
									"memory": "2Gi",
								},
							},
							"devices": map[string]interface{}{
								"disks": []interface{}{
									map[string]interface{}{
										"name": "containerdisk",
										"disk": map[string]interface{}{"bus": "virtio"},
									},
									map[string]interface{}{
										"name": "cloudinit",
										"disk": map[string]interface{}{"bus": "virtio"},
									},
								},
								"hostDevices": []interface{}{
									map[string]interface{}{
										"name":       "rbln0",
										"deviceName": string(vmpAtomResourceName),
									},
								},
							},
						},
						"volumes": []interface{}{
							map[string]interface{}{
								"name": "containerdisk",
								"containerDisk": map[string]interface{}{
									"image": vmpContainerDiskImage,
								},
							},
							map[string]interface{}{
								"name": "cloudinit",
								"cloudInitNoCloud": map[string]interface{}{
									"userData": userData,
								},
							},
						},
					},
				},
			},
		},
	}
}

func findVirtLauncherPod(
	ctx context.Context,
	client *e2ek8s.CoreClient,
	namespace, vmName string,
) (*corev1.Pod, error) {
	pods, err := client.GetPodsByLabel(ctx, namespace, map[string]string{
		vmpVirtLauncherLabelKey: vmpVirtLauncherLabelValue,
	})
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("virt-launcher-%s-", vmName)
	for i := range pods {
		if strings.HasPrefix(pods[i].Name, prefix) {
			return &pods[i], nil
		}
	}
	return nil, nil
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func getVMIPhase(
	ctx context.Context,
	g Gomega,
	te *testenv.TestEnv,
	namespace, name string,
) string {
	vmi, err := te.DynamicClient.Resource(vmiGVR).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if kapierrors.IsNotFound(err) {
		return ""
	}
	g.Expect(err).NotTo(HaveOccurred())
	phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
	return phase
}

func getVMPrintableStatus(
	ctx context.Context,
	g Gomega,
	te *testenv.TestEnv,
	namespace, name string,
) string {
	vm, err := te.DynamicClient.Resource(vmGVR).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	status, _, _ := unstructured.NestedString(vm.Object, "status", "printableStatus")
	return status
}

func vmiSyncFailure(ctx context.Context, te *testenv.TestEnv, namespace, name string) string {
	vmi, err := te.DynamicClient.Resource(vmiGVR).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	conditions, _, _ := unstructured.NestedSlice(vmi.Object, "status", "conditions")
	for _, raw := range conditions {
		cond, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		reason, _, _ := unstructured.NestedString(cond, "reason")
		if condType == "Synchronized" && reason == "FailedCreate" {
			msg, _, _ := unstructured.NestedString(cond, "message")
			return fmt.Sprintf("%s: %s", reason, msg)
		}
	}
	return ""
}

func describeVMI(ctx context.Context, te *testenv.TestEnv, namespace, name string) string {
	vmi, err := te.DynamicClient.Resource(vmiGVR).Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Sprintf("VMI %s/%s not retrievable: %v", namespace, name, err)
	}
	phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
	var b strings.Builder
	fmt.Fprintf(&b, "VMI %s phase=%q conditions:", name, phase)
	conditions, _, _ := unstructured.NestedSlice(vmi.Object, "status", "conditions")
	for _, raw := range conditions {
		cond, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		status, _, _ := unstructured.NestedString(cond, "status")
		reason, _, _ := unstructured.NestedString(cond, "reason")
		msg, _, _ := unstructured.NestedString(cond, "message")
		fmt.Fprintf(&b, "\n  - %s=%s reason=%q message=%q", condType, status, reason, msg)
	}
	return b.String()
}
