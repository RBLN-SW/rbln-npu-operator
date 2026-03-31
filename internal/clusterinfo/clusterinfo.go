package clusterinfo

import (
	"context"
	"fmt"
	"strings"

	configv1 "github.com/openshift/api/config/v1"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	clusterVersionName      = "version"
	completedHistoryState   = "Completed"
	runtimePreferenceDocker = 2
	runtimePreferenceCRIO   = 1
	runtimePreferenceCtrd   = 0
)

var runtimePreference = map[string]int{
	consts.Containerd: runtimePreferenceCtrd,
	consts.CRIO:       runtimePreferenceCRIO,
	consts.Docker:     runtimePreferenceDocker,
}

type Info struct {
	OpenShiftVersion string
	ContainerRuntime string
}

func New(ctx context.Context, config *rest.Config) (*Info, error) {
	configClient, err := configv1client.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create openshift config client: %w", err)
	}

	coreClient, err := corev1client.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build k8s core v1 client: %w", err)
	}

	openShiftVersion, err := detectOpenShiftVersion(ctx, configClient)
	if err != nil {
		return nil, fmt.Errorf("detect openshift version: %w", err)
	}

	containerRuntime, err := detectContainerRuntime(ctx, coreClient.Nodes(), openShiftVersion)
	if err != nil {
		return nil, fmt.Errorf("detect container runtime: %w", err)
	}

	return &Info{
		OpenShiftVersion: openShiftVersion,
		ContainerRuntime: containerRuntime,
	}, nil
}

func detectOpenShiftVersion(ctx context.Context, configClient configv1client.ConfigV1Interface) (string, error) {
	clusterVersion, err := configClient.ClusterVersions().Get(ctx, clusterVersionName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}

	version, ok := completedOpenShiftVersion(clusterVersion.Status.History)
	if !ok {
		return "", fmt.Errorf("failed to find completed openshift cluster version")
	}

	return version, nil
}

func completedOpenShiftVersion(history []configv1.UpdateHistory) (string, bool) {
	for _, update := range history {
		if update.State != completedHistoryState {
			continue
		}

		return normalizeOpenShiftVersion(update.Version), true
	}

	return "", false
}

func normalizeOpenShiftVersion(version string) string {
	parts := strings.Split(version, ".")
	if len(parts) > 1 {
		return parts[0] + "." + parts[1]
	}

	return parts[0]
}

func detectContainerRuntime(
	ctx context.Context,
	nodesClient corev1client.NodeInterface,
	openShiftVersion string,
) (string, error) {
	if openShiftVersion != "" {
		return consts.CRIO, nil
	}

	nodes, err := listRuntimeCandidateNodes(ctx, nodesClient)
	if err != nil {
		return "", err
	}

	return chooseContainerRuntime(nodes), nil
}

func listRuntimeCandidateNodes(ctx context.Context, nodesClient corev1client.NodeInterface) ([]corev1.Node, error) {
	nodeSelector := labels.Set{consts.RBLNPresentLabelKey: "true"}.AsSelector().String()
	nodeList, err := nodesClient.List(ctx, metav1.ListOptions{LabelSelector: nodeSelector})
	if err != nil {
		return nil, fmt.Errorf("list nodes prior to checking container runtime: %w", err)
	}

	if len(nodeList.Items) > 0 {
		return nodeList.Items, nil
	}

	nodeList, err = nodesClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes for container runtime fallback: %w", err)
	}

	return nodeList.Items, nil
}

func chooseContainerRuntime(nodes []corev1.Node) string {
	bestRuntime := ""
	bestPreference := len(runtimePreference)

	for _, node := range nodes {
		runtime, err := getRuntimeString(node)
		if err != nil {
			continue
		}

		preference := runtimePreference[runtime]
		if preference < bestPreference {
			bestRuntime = runtime
			bestPreference = preference
		}
	}

	if bestRuntime == "" {
		return consts.Containerd
	}

	return bestRuntime
}

func getRuntimeString(node corev1.Node) (string, error) {
	runtimeVersion := node.Status.NodeInfo.ContainerRuntimeVersion

	switch {
	case strings.HasPrefix(runtimeVersion, "docker"):
		return consts.Docker, nil
	case strings.HasPrefix(runtimeVersion, "containerd"):
		return consts.Containerd, nil
	case strings.HasPrefix(runtimeVersion, "cri-o"):
		return consts.CRIO, nil
	default:
		return "", fmt.Errorf("runtime not recognized: %s", runtimeVersion)
	}
}
