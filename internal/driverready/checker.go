package driverready

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// Checker treats the host marker as an installation signal, not as evidence
// that the container which published it is still running.
type Checker struct {
	client              kubernetes.Interface
	node                string
	namespace           string
	hostDriverValidated func() (bool, error)
}

func New(node, namespace string, hostDriverValidated func() (bool, error)) (*Checker, error) {
	if node == "" || namespace == "" {
		return nil, fmt.Errorf("NODE_NAME and OPERATOR_NAMESPACE are required for driver readiness")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load driver readiness client config: %w", err)
	}
	cfg.Timeout = 10 * time.Second
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create driver readiness client: %w", err)
	}
	return &Checker{client: c, node: node, namespace: namespace, hostDriverValidated: hostDriverValidated}, nil
}

func (c *Checker) Check(ctx context.Context) error {
	node, err := c.client.CoreV1().Nodes().Get(ctx, c.node, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get driver node: %w", err)
	}
	owner := node.Labels[consts.RBLNDriverOwnerLabelKey]
	if owner != "" && node.Labels[consts.RBLNDeployDriverLabelKey] != "true" {
		return fmt.Errorf("node %s has no active containerized driver owner", c.node)
	}
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", c.node).String(),
		LabelSelector: labels.SelectorFromSet(labels.Set{
			"app.kubernetes.io/component": "rbln-driver",
		}).String(),
	})
	if err != nil {
		return fmt.Errorf("list driver pods: %w", err)
	}
	if owner != "" {
		return checkPods(pods.Items, c.node, owner)
	}
	// A host may never have had a driver CR (driver.enabled=false), so the
	// manager's pre-installed label is neither required nor sufficient.
	for i := range pods.Items {
		if isDriverPodForNode(&pods.Items[i], c.node) {
			return fmt.Errorf("node %s still has a containerized driver pod", c.node)
		}
	}
	if c.hostDriverValidated != nil {
		validated, err := c.hostDriverValidated()
		if err != nil {
			return err
		}
		if validated {
			return nil
		}
	}
	return fmt.Errorf("node %s has neither a driver owner nor a validated host driver", c.node)
}

func isDriverPodForNode(p *corev1.Pod, node string) bool {
	return p.Spec.NodeName == node && p.Labels["app.kubernetes.io/component"] == "rbln-driver" &&
		p.Status.Phase != corev1.PodFailed && p.Status.Phase != corev1.PodSucceeded
}

func checkPods(pods []corev1.Pod, node, owner string) error {
	var current *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if !isDriverPodForNode(p, node) || p.DeletionTimestamp != nil || p.Labels["rebellions.ai/driver-instance"] != owner {
			continue
		}
		if current != nil {
			return fmt.Errorf("multiple active driver pods on node %s", node)
		}
		current = p
	}
	if current == nil {
		return fmt.Errorf("no active driver pod on node %s", node)
	}
	if current.Status.Phase == corev1.PodRunning {
		for _, status := range current.Status.ContainerStatuses {
			if status.Name == consts.DriverContainerName && status.State.Running != nil && status.Ready {
				return nil
			}
		}
	}
	return fmt.Errorf("driver container in pod %s is not Running and Ready", current.Name)
}
