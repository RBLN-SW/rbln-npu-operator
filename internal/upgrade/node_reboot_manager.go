package upgrade

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type RebootTriggerRequest struct {
	RequestedAtUnix int64
	PreRebootBootID string
	Namespace       string
	PodName         string
	Image           string
}

type RebootManager interface {
	Trigger(ctx context.Context, node *corev1.Node, req RebootTriggerRequest) error
}

type PodRebootManager struct {
	k8sClient   ctrlclient.Client
	rebootImage string
	log         logr.Logger
}

func NewPodRebootManager(k8sClient ctrlclient.Client, log logr.Logger) *PodRebootManager {
	image := os.Getenv("RBLN_NODE_REBOOT_IMAGE")
	if image == "" {
		image = "harbor.k8s.rebellions.in/rebellions/rbln-node-reboot:v1.0.0"
	}
	return &PodRebootManager{
		k8sClient:   k8sClient,
		rebootImage: image,
		log:         log,
	}
}

func (m *PodRebootManager) Trigger(
	ctx context.Context, node *corev1.Node, req RebootTriggerRequest,
) error {
	rebootImage := req.Image
	if rebootImage == "" {
		rebootImage = m.rebootImage
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.PodName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "rbln-node-reboot",
				"app.kubernetes.io/managed-by": "rbln-npu-operator",
			},
		},
		Spec: corev1.PodSpec{
			HostPID:       true,
			HostNetwork:   true,
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      node.Name,
			Containers: []corev1.Container{
				{
					Name:            "reboot-container",
					Image:           rebootImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command: []string{
						"/usr/bin/nsenter",
						"--all",
						"--target=1",
						"--",
						"reboot",
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: ptrTo(true),
					},
				},
			},
		},
	}

	m.log.Info(
		"Triggering reboot via node-bound pod",
		"node", node.Name,
		"namespace", req.Namespace,
		"pod", req.PodName,
		"requestedAt", req.RequestedAtUnix,
		"preRebootBootID", req.PreRebootBootID,
		"image", rebootImage,
	)

	err := m.k8sClient.Create(ctx, pod)
	if apierrors.IsAlreadyExists(err) {
		m.log.Info("Reboot pod already exists, treating as already-triggered", "namespace", req.Namespace, "pod", req.PodName)
		return nil
	}
	if err != nil {
		return err
	}

	return nil
}

func ptrTo[T any](v T) *T {
	return &v
}

func BuildRebootPodName(nodeName string, requestedAtUnix int64) string {
	return fmt.Sprintf("rbln-reboot-%s-%d", nodeName, requestedAtUnix)
}

func (m *PodRebootManager) DeleteRebootPod(
	ctx context.Context, namespace string, podName string,
) error {
	if namespace == "" || podName == "" {
		return nil
	}
	err := m.k8sClient.Delete(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
		},
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
