package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"

	"github.com/spf13/cobra"
)

// componentWaitsForPartition is the component×node gate table: every gated
// component waits for toolkit-ready; components that advertise NPU devices
// must additionally wait for partition-ready on partitioned nodes, so they
// never expose a full PF that is about to be carved into VFs. partition-ready
// is written by the partition-manager once VF partitioning is applied, so the
// partition-manager itself must never wait for it — it would deadlock on the
// marker it owns. Until the manager runs, blocking device advertisement on
// partitioned nodes is the intended fail-closed behavior.
var componentWaitsForPartition = map[string]bool{
	consts.RBLNDevicePluginName:     true,
	consts.RBLNDRAKubeletPluginName: true,
	consts.RBLNMetricExporterName:   false,
	consts.RBLNFeatureDiscoveryName: false,
	consts.RBLNDaemonName:           false,
	consts.RBLNPartitionManagerName: false,
}

type gateRuntime struct {
	getNodeLabels func(ctx context.Context, nodeName string) (map[string]string, error)
	sleep         func(time.Duration)
}

func defaultGateRuntime() gateRuntime {
	return gateRuntime{
		getNodeLabels: getNodeLabelsFromAPI,
		sleep:         time.Sleep,
	}
}

func getNodeLabelsFromAPI(ctx context.Context, nodeName string) (map[string]string, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get node %s: %w", nodeName, err)
	}
	return node.GetLabels(), nil
}

func newGateCommand(config *rootConfig) *cobra.Command {
	var component string
	cmd := &cobra.Command{
		Use:   "gate",
		Short: "Block until the component's readiness files for this node exist",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGate(cmd.Context(), config.gateConfig(component), defaultGateRuntime())
		},
	}
	cmd.Flags().StringVar(&component, "component", "", "component name to gate")
	_ = cmd.MarkFlagRequired("component")
	return cmd
}

// runGate re-reads the node's partition labels on every poll, so a label
// change during the wait (partitioning requested or aborted) is honored
// without a pod restart. Fail-closed: on API errors or invalid label values
// it keeps retrying instead of assuming an unpartitioned node — misreading a
// partitioned node would let components advertise full PFs that are about to
// be carved into VFs.
func runGate(ctx context.Context, cfg *gateConfig, rt gateRuntime) error {
	waitsForPartition, ok := componentWaitsForPartition[cfg.component]
	if !ok {
		return fmt.Errorf("unknown component %q", cfg.component)
	}
	if cfg.nodeName == "" {
		return errors.New("NODE_NAME environment variable is required")
	}

	interval := time.Duration(cfg.sleepIntervalSeconds) * time.Second

	for {
		labels, err := rt.getNodeLabels(ctx, cfg.nodeName)
		if err != nil {
			slog.Error("failed to get node labels", "node", cfg.nodeName, "err", err, "sleepSeconds", cfg.sleepIntervalSeconds)
			rt.sleep(interval)
			continue
		}

		partitioned, err := nodeIsPartitioned(labels)
		if err != nil {
			slog.Error(
				"invalid partition labels, fix the node labels to unblock",
				"node", cfg.nodeName, "err", err, "sleepSeconds", cfg.sleepIntervalSeconds,
			)
			rt.sleep(interval)
			continue
		}

		readyFiles := []string{toolkitReadyFile}
		if waitsForPartition && partitioned {
			readyFiles = append(readyFiles, consts.PartitionReadyFileName)
		}

		missing := statusfile.Missing(cfg.outputDir, readyFiles)
		if len(missing) == 0 {
			return nil
		}
		slog.Info("waiting for status files", "dir", cfg.outputDir, "missing", missing, "partitioned", partitioned)
		rt.sleep(interval)
	}
}

// nodeIsPartitioned reports whether any NPU on the node is configured for VF
// partitioning via the rebellions.ai/npu.partition[.<idx>] labels. Unknown
// mode tokens or malformed NPU indexes are an error so callers block until
// the admin fixes the label, instead of silently treating the node as
// unpartitioned.
func nodeIsPartitioned(labels map[string]string) (bool, error) {
	partitioned := false
	for key, value := range labels {
		switch {
		case key == consts.RBLNPartitionLabelKey:
		case strings.HasPrefix(key, consts.RBLNPartitionIndexLabelPrefix):
			idx, err := strconv.Atoi(strings.TrimPrefix(key, consts.RBLNPartitionIndexLabelPrefix))
			if err != nil || idx < 0 {
				return false, fmt.Errorf("invalid NPU index in label %q", key)
			}
		default:
			continue
		}

		switch value {
		case consts.RBLNPartitionModeVF1, consts.RBLNPartitionModeVF4:
			partitioned = true
		case consts.RBLNPartitionModeNone:
		default:
			return false, fmt.Errorf("invalid %s label value %q", key, value)
		}
	}
	return partitioned, nil
}
