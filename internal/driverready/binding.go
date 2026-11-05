package driverready

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"

	driverapi "github.com/RBLN-SW/rbln-npu-operator/api/v1alpha1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

const deployedLabelValue = "true"

var driverResource = schema.GroupVersionResource{
	Group: driverapi.GroupVersion.Group, Version: driverapi.GroupVersion.Version, Resource: "rblndrivers",
}

// VM gates must also wait for a managed installation: VFIO bindings and a
// partition marker left by an earlier run are not evidence of driver readiness.
// A VFIO-only node without a managed driver keeps its existing gate.
func (c *Checker) CheckForBinding(ctx context.Context, root string) error {
	node, err := c.client.CoreV1().Nodes().Get(ctx, c.node, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + c.node, LabelSelector: "app.kubernetes.io/component=rbln-driver"})
	if err != nil {
		return err
	}
	owner := node.Labels[consts.RBLNDriverOwnerLabelKey]
	active := false
	for i := range pods.Items {
		if isDriverPodForNode(&pods.Items[i], c.node) {
			active = true
		}
	}
	if owner == "" && !active {
		if c.hostDriverValidated != nil {
			valid, err := c.hostDriverValidated()
			if err != nil {
				return err
			}
			if valid {
				return nil
			}
		}
		if node.Labels[consts.RBLNDeployDriverLabelKey] != deployedLabelValue {
			return nil
		}
		// The deploy label is eligibility, not evidence of a desired driver.
		// Check configuration so unowned nodes work without racing a pending
		// owner assignment for a driver that actually targets this node.
		expected, err := c.driverConfiguredForNode(ctx, node)
		if err != nil {
			return err
		}
		if !expected {
			return nil
		}
		return fmt.Errorf("managed driver has not been assigned")
	}
	if owner == "" || node.Labels[consts.RBLNDeployDriverLabelKey] != deployedLabelValue {
		return fmt.Errorf("managed driver ownership unavailable")
	}
	if err := checkPods(pods.Items, c.node, owner); err != nil {
		return err
	}
	_, err = os.Stat(filepath.Join(root, ".driver-ctr-ready"))
	return err
}

func (c *Checker) driverConfiguredForNode(ctx context.Context, node *corev1.Node) (bool, error) {
	if c.drivers == nil {
		return false, fmt.Errorf("driver configuration reader is unavailable")
	}
	list, err := c.drivers.Resource(driverResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list driver configurations: %w", err)
	}
	for _, item := range list.Items {
		var instance driverapi.RBLNDriver
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, &instance); err != nil {
			return false, fmt.Errorf("read driver configuration %s: %w", item.GetName(), err)
		}
		// Match the owner resolver's routable-candidate rules. Any matching
		// candidate (including an ownership conflict) keeps the gate closed.
		if instance.DeletionTimestamp != nil || len(instance.Name) > validation.LabelValueMaxLength {
			continue
		}
		if _, reserved := instance.Spec.NodeSelector[consts.RBLNDriverOwnerLabelKey]; reserved {
			continue
		}
		if _, reserved := instance.Spec.NodeSelector[consts.RBLNDeployDriverLabelKey]; reserved {
			continue
		}
		if labels.SelectorFromSet(instance.Spec.NodeSelector).Matches(labels.Set(node.Labels)) {
			return true, nil
		}
	}
	return false, nil
}
