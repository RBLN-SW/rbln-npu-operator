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
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	e2ek8s "github.com/rebellions-sw/rbln-npu-operator/test/e2e/kubernetes"
	e2elog "github.com/rebellions-sw/rbln-npu-operator/test/e2e/logs"
	"github.com/rebellions-sw/rbln-npu-operator/test/e2e/testenv"
)

const (
	defaultOperandPollInterval = 5 * time.Second
	defaultOperandWaitTimeout  = 15 * time.Minute
	NPUResourceName            = corev1.ResourceName("rebellions.ai/npu")
	draDeviceClassName         = "npu.rebellions.ai"
	devicePluginNodeLabelKey   = "rebellions.ai/npu.deploy.device-plugin"
	devicePluginNodeLabelValue = "true"
	appComponentLabelKey       = "app.kubernetes.io/component"
	rblnClusterPolicyCRDName   = "rblnclusterpolicies.rebellions.ai"
	rblnDriverCRDName          = "rblndrivers.rebellions.ai"
	registryServer             = "repo.rebellions.ai"
	registrySecretName         = "drivercred"
)

var _ = Describe("e2e-npu-operator-scenario-test", Ordered, func() {
	te := testenv.NewTestEnv()

	Describe("NPU Operator RBLNClusterPolicy", func() {
		Context("Container-type NPU Operator deployment", Ordered, func() {
			/*
			   Scenario:
			   - Deploy NPU Operator CRD with container-type configuration.
			   - Verify that all operator-managed components (device-plugin, feature-discovery, metrics-exporter.)
			     are successfully created, running, and reporting healthy states.
			   - Validate CR status conditions, DaemonSet/Pod readiness, and functional behavior.
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
					"HelmReleaseName",
					buildOperatorHelmValues(true, false),
				)
				setupSucceeded = true
			})

			AfterAll(func(ctx context.Context) {
				if setupSucceeded {
					err := helmClient.Uninstall(ctx, helmReleaseName)
					if err != nil {
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

			It("should bring up the all of the operand pods successfully", func(ctx context.Context) {
				waitForPodsReady(ctx, k8sCoreClient, testNamespace.Name, "rbln-driver", map[string]string{
					appComponentLabelKey: "rbln-driver",
				})

				operands := []struct {
					name      string
					component string
				}{
					{"rbln-device-plugin", "device-plugin"},
					{"rbln-metrics-exporter", "metrics-exporter"},
					{"rbln-npu-feature-discovery", "npu-feature-discovery"},
				}
				e2elog.Infof("Ensure that the npu operator operands come up")
				for _, operand := range operands {
					waitForPodsReady(ctx, k8sCoreClient, testNamespace.Name, operand.name, map[string]string{
						appComponentLabelKey: operand.component,
					})
				}
			})

			It("should advertise rebellions.ai/npu on ready nodes", func(ctx context.Context) {
				Eventually(func(g Gomega) bool {
					nodes, err := k8sCoreClient.ListNodes(ctx, map[string]string{
						devicePluginNodeLabelKey: devicePluginNodeLabelValue,
					})
					g.Expect(err).NotTo(HaveOccurred())

					found := false
					for i := range nodes {
						node := &nodes[i]
						if !k8sCoreClient.IsNodeReady(node) {
							continue
						}

						allocQty, allocOK := node.Status.Allocatable[NPUResourceName]
						if !allocOK || allocQty.Value() == 0 {
							e2elog.Infof("node %s has no allocatable %s", node.Name, NPUResourceName)
							return false
						}

						capQty, capOK := node.Status.Capacity[NPUResourceName]
						if !capOK || capQty.Value() == 0 {
							e2elog.Infof("node %s has no capacity %s", node.Name, NPUResourceName)
							return false
						}

						found = true
					}
					return found
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "no ready labeled node exposed rebellions.ai/npu")
			})

			It("should run model-zoo compile/inference on ubuntu 24.04", func(ctx context.Context) {
				ensurePyPISecret(ctx, te.ClientSet.CoreV1(), testNamespace.Name, e2eCfg.pypiUsername, e2eCfg.pypiPassword)
				DeferCleanup(func() {
					cleanupPyPISecret(te.ClientSet.CoreV1(), testNamespace.Name)
				})

				podName := "model-zoo-ubuntu-24-04"
				container := newModelZooContainer()
				container.Resources = corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						NPUResourceName: resource.MustParse("1"),
					},
				}

				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: testNamespace.Name,
						Labels:    map[string]string{"app": "model-zoo-smoke"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{container},
					},
				}

				_, err := te.ClientSet.CoreV1().Pods(testNamespace.Name).Create(ctx, pod, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					_ = te.ClientSet.CoreV1().Pods(testNamespace.Name).Delete(context.Background(), podName, metav1.DeleteOptions{})
				})

				waitForPodCompletion(ctx, te.ClientSet.CoreV1(), testNamespace.Name, podName)
			})

			It("should switch from device-plugin to DRA kubelet plugin", func(ctx context.Context) {
				err := helmClient.Upgrade(ctx, helmReleaseName, ChartOptions{
					CleanupOnFail: true,
					Timeout:       5 * time.Minute,
					Wait:          true,
					Values:        buildOperatorHelmValues(false, true),
				})
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) bool {
					pods, err := k8sCoreClient.GetPodsByLabel(ctx, testNamespace.Name, map[string]string{
						appComponentLabelKey: "device-plugin",
					})
					g.Expect(err).NotTo(HaveOccurred())
					return len(pods) == 0
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "device-plugin pods were not removed")

				waitForPodsReady(ctx, k8sCoreClient, testNamespace.Name, "rbln-dra-kubelet-plugin", map[string]string{
					appComponentLabelKey: "dra-kubelet-plugin",
				})
			})

			It("should reconcile DRA DeviceClass", func(ctx context.Context) {
				Eventually(func(g Gomega) bool {
					_, err := te.ClientSet.ResourceV1().DeviceClasses().Get(ctx, draDeviceClassName, metav1.GetOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					return true
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "DRA DeviceClass %s not found", draDeviceClassName)
			})

			It("should advertise DRA ResourceSlice", func(ctx context.Context) {
				Eventually(func(g Gomega) bool {
					slices, err := te.ClientSet.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
					g.Expect(err).NotTo(HaveOccurred())

					for _, slice := range slices.Items {
						if slice.Spec.Driver == draDeviceClassName {
							return true
						}
					}
					return false
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "DRA ResourceSlice for driver %s not found", draDeviceClassName)
			})

			It("should run model-zoo compile/inference on ubuntu 24.04 with DRA", func(ctx context.Context) {
				ensurePyPISecret(ctx, te.ClientSet.CoreV1(), testNamespace.Name, e2eCfg.pypiUsername, e2eCfg.pypiPassword)
				DeferCleanup(func() {
					cleanupPyPISecret(te.ClientSet.CoreV1(), testNamespace.Name)
				})

				claimTemplateName := "model-zoo-dra-claim-template"
				claimTemplate := &resourcev1.ResourceClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Name:      claimTemplateName,
						Namespace: testNamespace.Name,
					},
					Spec: resourcev1.ResourceClaimTemplateSpec{
						Spec: resourcev1.ResourceClaimSpec{
							Devices: resourcev1.DeviceClaim{
								Requests: []resourcev1.DeviceRequest{
									{
										Name: "npu",
										Exactly: &resourcev1.ExactDeviceRequest{
											DeviceClassName: draDeviceClassName,
											Count:           1,
										},
									},
								},
							},
						},
					},
				}
				_, err := te.ClientSet.ResourceV1().
					ResourceClaimTemplates(testNamespace.Name).
					Create(ctx, claimTemplate, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					_ = te.ClientSet.ResourceV1().ResourceClaimTemplates(testNamespace.Name).
						Delete(context.Background(), claimTemplateName, metav1.DeleteOptions{})
				})

				podName := "model-zoo-ubuntu-24-04-dra"
				container := newModelZooContainer()
				container.Resources = corev1.ResourceRequirements{
					Claims: []corev1.ResourceClaim{
						{Name: "npu", Request: "npu"},
					},
				}

				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: testNamespace.Name,
						Labels:    map[string]string{"app": "model-zoo-smoke-dra"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						ResourceClaims: []corev1.PodResourceClaim{
							{
								Name:                      "npu",
								ResourceClaimTemplateName: stringPtr(claimTemplateName),
							},
						},
						Containers: []corev1.Container{container},
					},
				}

				_, err = te.ClientSet.CoreV1().Pods(testNamespace.Name).Create(ctx, pod, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					_ = te.ClientSet.CoreV1().Pods(testNamespace.Name).Delete(context.Background(), podName, metav1.DeleteOptions{})
				})

				var generatedClaimName string
				Eventually(func(g Gomega) bool {
					pod, err := te.ClientSet.CoreV1().Pods(testNamespace.Name).Get(ctx, podName, metav1.GetOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					for _, claimStatus := range pod.Status.ResourceClaimStatuses {
						if claimStatus.Name != "npu" || claimStatus.ResourceClaimName == nil {
							continue
						}

						generatedClaimName = *claimStatus.ResourceClaimName
						return true
					}
					return false
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "generated resource claim for pod %s not found", podName)

				Eventually(func(g Gomega) bool {
					rc, err := te.ClientSet.ResourceV1().
						ResourceClaims(testNamespace.Name).
						Get(ctx, generatedClaimName, metav1.GetOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					return rc.Status.Allocation != nil
				}).WithContext(ctx).
					WithPolling(defaultOperandPollInterval).
					Within(defaultOperandWaitTimeout).
					Should(BeTrue(), "resource claim %s was not allocated", generatedClaimName)

				waitForPodCompletion(ctx, te.ClientSet.CoreV1(), testNamespace.Name, podName)
			})
		})
	})
})

// ---------------------------------------------------------------------------
// Pod readiness helper
// ---------------------------------------------------------------------------

func waitForPodsReady(
	ctx context.Context, client *e2ek8s.CoreClient, namespace, target string, labelMap map[string]string,
) {
	By(fmt.Sprintf("waiting for %s pods to become ready", target))
	Eventually(func() bool {
		pods, err := client.GetPodsByLabel(ctx, namespace, labelMap)
		if err != nil {
			e2elog.Infof("WARN: error retrieving pods for %s: %v", target, err)
			return false
		}

		var readyCount int
		for _, pod := range pods {
			e2elog.Infof("Checking status of pod %s for target %s", pod.Name, target)
			isReady, err := client.IsPodReady(ctx, pod.Name, pod.Namespace)
			if err != nil {
				e2elog.Infof("WARN: error when retrieving pod status of %s/%s: %v", namespace, target, err)
				return false
			}
			if isReady {
				readyCount++
			}
		}
		return len(pods) > 0 && readyCount == len(pods)
	}).WithPolling(defaultOperandPollInterval).
		Within(defaultOperandWaitTimeout).
		WithContext(ctx).
		Should(BeTrue())
}

// ---------------------------------------------------------------------------
// Docker registry helpers
// ---------------------------------------------------------------------------

type dockerAuthConfig struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Email    string `json:"email,omitempty"`
}

type dockerConfig struct {
	Auths map[string]dockerAuthConfig `json:"auths"`
}

func buildOperatorHelmValues(devicePluginEnabled, draKubeletPluginEnabled bool) map[string]interface{} {
	return map[string]interface{}{
		"operator": map[string]interface{}{
			"image": map[string]interface{}{
				"registry":   e2eCfg.operatorRegistry,
				"repository": e2eCfg.operatorRepository,
				"tag":        e2eCfg.operatorVersion,
			},
		},
		"driver": map[string]interface{}{
			"enabled":          true,
			"imagePullSecrets": []string{registrySecretName},
		},
		"devicePlugin": map[string]interface{}{
			"enabled": devicePluginEnabled,
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
		"draKubeletPlugin": map[string]interface{}{
			"enabled":    draKubeletPluginEnabled,
			"driverName": draDeviceClassName,
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
		"metricsExporter": map[string]interface{}{
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
		"rblnDaemon": map[string]interface{}{
			"enabled":          true,
			"imagePullSecrets": []string{registrySecretName},
		},
		"validator": map[string]interface{}{
			"image": map[string]interface{}{
				"registry":   e2eCfg.validatorRegistry,
				"repository": e2eCfg.validatorRepository,
				"tag":        e2eCfg.operatorVersion,
			},
		},
		"npuFeatureDiscovery": map[string]interface{}{
			"image": map[string]interface{}{
				"pullPolicy": "Always",
			},
		},
	}
}

func stringPtr(s string) *string {
	return &s
}

func setupOperatorDeployment(
	ctx context.Context,
	te *testenv.TestEnv,
	releaseName string,
	releaseLogLabel string,
	helmValues map[string]interface{},
) (*HelmClient, string, *e2ek8s.CoreClient, *corev1.Namespace) {
	var err error
	k8sCoreClient := e2ek8s.NewClient(te.ClientSet.CoreV1())
	nsLabels := map[string]string{
		"e2e-run": string(testenv.RunID),
	}

	testNamespace, err := k8sCoreClient.CreateNamespace(ctx, e2eCfg.namespace, nsLabels)
	if err != nil {
		Fail(fmt.Sprintf("failed to create operator namespace %s: %v", e2eCfg.namespace, err))
	}

	if e2eCfg.registryUser == "" || e2eCfg.registryPassword == "" {
		Fail("registry credentials are required: set E2E_CONTAINER_REGISTRY_USER and E2E_CONTAINER_REGISTRY_PASSWORD")
	}
	if err := ensureRegistrySecret(
		ctx,
		te.ClientSet.CoreV1(),
		testNamespace.Name,
		e2eCfg.registryUser,
		e2eCfg.registryPassword,
	); err != nil {
		Fail(fmt.Sprintf("failed to create registry secret %s: %v", registrySecretName, err))
	}

	helmClient, err := NewHelmClient(
		testNamespace.Name,
		testenv.TestCtx.KubeConfig,
		e2eCfg.helmChart,
	)
	if err != nil {
		Fail(fmt.Sprintf("failed to instantiate helm client: %v", err))
	}

	helmReleaseName, err := helmClient.Install(ctx, ChartOptions{
		CleanupOnFail: true,
		ReleaseName:   releaseName,
		Timeout:       5 * time.Minute,
		Wait:          true,
		Values:        helmValues,
	})
	e2elog.Infof("%s: %s", releaseLogLabel, helmReleaseName)
	Expect(err).NotTo(HaveOccurred())

	return helmClient, helmReleaseName, k8sCoreClient, testNamespace
}

func buildDockerConfigJSON(username, password, email string) ([]byte, error) {
	config := dockerConfig{
		Auths: map[string]dockerAuthConfig{
			registryServer: {
				Username: username,
				Password: password,
				Email:    email,
			},
		},
	}
	return json.Marshal(config)
}

func ensureRegistrySecret(
	ctx context.Context,
	client corev1client.CoreV1Interface,
	namespace string,
	username string,
	password string,
) error {
	configJSON, err := buildDockerConfigJSON(username, password, "devops@rebellions.ai")
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      registrySecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: configJSON,
		},
	}

	if _, err := client.Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err == nil {
		return nil
	} else if !kapierrors.IsAlreadyExists(err) {
		return err
	}

	existing, err := client.Secrets(namespace).Get(ctx, registrySecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	secret.ResourceVersion = existing.ResourceVersion
	_, err = client.Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
	return err
}
