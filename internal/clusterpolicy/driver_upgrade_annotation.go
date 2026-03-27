package clusterpolicy

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

const (
	driverAutoUpgradeAnnotationKey = "rebellions.ai/npu-driver-upgrade-enabled"
)

func ReconcileDriverAutoUpgradeAnnotations(
	ctx context.Context,
	k8sClient client.Client,
	policy *rblnv1beta1.RBLNClusterPolicy,
	nodeList *corev1.NodeList,
) error {
	shouldEnable := shouldEnableDriverAutoUpgrade(policy)

	for i := range nodeList.Items {
		if !hasRBLNPresentLabel(nodeList.Items[i].GetLabels()) {
			continue
		}
		if err := reconcileDriverAutoUpgradeAnnotationForNode(
			ctx,
			k8sClient,
			&nodeList.Items[i],
			shouldEnable,
		); err != nil {
			return err
		}
	}

	return nil
}

func shouldEnableDriverAutoUpgrade(policy *rblnv1beta1.RBLNClusterPolicy) bool {
	return policy.Spec.Driver.UpgradePolicy != nil &&
		policy.Spec.Driver.UpgradePolicy.AutoUpgrade &&
		!policy.Spec.SandboxDevicePlugin.IsEnabled()
}

func reconcileDriverAutoUpgradeAnnotationForNode(
	ctx context.Context,
	k8sClient client.Client,
	node *corev1.Node,
	shouldEnable bool,
) error {
	if hasRBLNDeploySkipLabel(node.GetLabels()) {
		shouldEnable = false
	}

	annotations := ensureNodeAnnotations(node)

	if isDriverAutoUpgradeAnnotationReconciled(annotations, shouldEnable) {
		return nil
	}

	reconcileDriverAutoUpgradeAnnotation(annotations, shouldEnable)
	return updateNodeAnnotations(ctx, k8sClient, node, annotations)
}

func ensureNodeAnnotations(node *corev1.Node) map[string]string {
	annotations := node.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	return annotations
}

func isDriverAutoUpgradeAnnotationReconciled(
	annotations map[string]string,
	shouldEnable bool,
) bool {
	currentValue, exists := annotations[driverAutoUpgradeAnnotationKey]
	if shouldEnable {
		return exists && currentValue == labelValueTrue
	}
	return !exists
}

func reconcileDriverAutoUpgradeAnnotation(
	annotations map[string]string,
	shouldEnable bool,
) {
	if shouldEnable {
		annotations[driverAutoUpgradeAnnotationKey] = labelValueTrue
		return
	}

	delete(annotations, driverAutoUpgradeAnnotationKey)
}

func updateNodeAnnotations(
	ctx context.Context,
	k8sClient client.Client,
	node *corev1.Node,
	annotations map[string]string,
) error {
	node.SetAnnotations(annotations)
	if err := k8sClient.Update(ctx, node); err != nil {
		return fmt.Errorf("update node %s annotations: %w", node.Name, err)
	}
	return nil
}
