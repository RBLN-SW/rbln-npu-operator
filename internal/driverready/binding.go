package driverready

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

const deployedLabelValue = "true"

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
