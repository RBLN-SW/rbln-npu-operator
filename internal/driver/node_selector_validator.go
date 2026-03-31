package driver

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

type NodeSelectorValidator interface {
	Validate(ctx context.Context, instance *rebellionsaiv1alpha1.RBLNDriver) error
}

type nodeSelectorValidator struct {
	client client.Client
}

func NewNodeSelectorValidator(client client.Client) NodeSelectorValidator {
	return &nodeSelectorValidator{client: client}
}

func (v *nodeSelectorValidator) Validate(ctx context.Context, instance *rebellionsaiv1alpha1.RBLNDriver) error {
	currentSelector := instance.GetNodeSelector()
	nodes := &corev1.NodeList{}
	if err := v.client.List(ctx, nodes, client.MatchingLabels(currentSelector)); err != nil {
		return fmt.Errorf("list nodes for selector validation: %w", err)
	}
	if len(nodes.Items) == 0 {
		return nil
	}

	driverList := &rebellionsaiv1alpha1.RBLNDriverList{}
	if err := v.client.List(ctx, driverList); err != nil {
		return fmt.Errorf("list RBLNDriver resources for selector validation: %w", err)
	}

	for _, d := range driverList.Items {
		if d.Name == instance.Name && d.Namespace == instance.Namespace {
			continue
		}
		otherSelector := d.GetNodeSelector()
		for _, node := range nodes.Items {
			if labelsMatch(node.Labels, otherSelector) {
				return fmt.Errorf("nodeSelector conflicts with RBLNDriver %q", d.Name)
			}
		}
	}

	return nil
}

func labelsMatch(labels map[string]string, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}
