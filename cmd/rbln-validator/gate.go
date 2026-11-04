package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	drivervalidator "github.com/RBLN-SW/rbln-npu-operator/cmd/rbln-validator/driver"
	"github.com/RBLN-SW/rbln-npu-operator/cmd/rbln-validator/statusfile"
	vfiovalidator "github.com/RBLN-SW/rbln-npu-operator/cmd/rbln-validator/vfiopci"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/driverready"

	"github.com/spf13/cobra"
)

// Device advertisers wait for the partition manager to finish driver binding
// and VF topology changes. The manager itself owns the marker and must not wait
// for it. VM gates also validate the resulting PF-only or VF-only PCI layout.
var componentWaitsForPartition = map[string]bool{
	consts.RBLNDevicePluginName:        true,
	consts.RBLNSandboxDevicePluginName: true,
	consts.RBLNDRAKubeletPluginName:    true,
	consts.RBLNMetricExporterName:      false,
	consts.RBLNFeatureDiscoveryName:    false,
	consts.RBLNPartitionManagerName:    false,
}

type gateRuntime struct {
	getNodeLabels      func(ctx context.Context, nodeName string) (map[string]string, error)
	validateVFIO       func(vfiovalidator.Config) (vfiovalidator.Result, error)
	checkDriver        func(context.Context) error
	checkBindingDriver func(context.Context) error
	sleep              func(time.Duration)
}

func defaultGateRuntime() gateRuntime {
	return gateRuntime{
		getNodeLabels: getNodeLabelsFromAPI,
		validateVFIO:  vfiovalidator.Validate,
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
			cfg := config.gateConfig(component)
			checker, err := driverready.New(cfg.nodeName, os.Getenv(envOperatorNamespace), func() (bool, error) {
				return drivervalidator.HostDriverValidated(cfg.outputDir)
			})
			if err != nil {
				return err
			}
			rt := defaultGateRuntime()
			rt.checkDriver = checker.Check
			rt.checkBindingDriver = func(ctx context.Context) error { return checker.CheckForBinding(ctx, cfg.outputDir) }
			return runGate(cmd.Context(), cfg, rt)
		},
	}
	cmd.Flags().StringVar(&component, "component", "", "component name to gate")
	_ = cmd.MarkFlagRequired("component")
	return cmd
}

// Except for the driver-only partition-manager gate, runGate re-reads the
// node's partition labels on every poll, so a label
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
	if cfg.component == consts.RBLNPartitionManagerName && rt.checkBindingDriver == nil {
		return errors.New("partition-manager gate requires a driver readiness checker")
	}

	interval := time.Duration(cfg.sleepIntervalSeconds) * time.Second
	slog.Info("gate started", "component", cfg.component, "node", cfg.nodeName)

	for {
		if cfg.component == consts.RBLNPartitionManagerName {
			if err := rt.checkBindingDriver(ctx); err == nil {
				slog.Info("driver readiness gate passed", "component", cfg.component)
				return nil
			} else {
				slog.Info("waiting for current driver installation", "err", err)
			}
			rt.sleep(interval)
			continue
		}
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

		// Match the operator's workload resolution: a valid node label wins,
		// otherwise the policy default determines which stack runs here.
		workload := labels[consts.RBLNWorkloadConfigLabelKey]
		if workload != consts.RBLNWorkloadConfigContainer && workload != consts.RBLNWorkloadConfigVMPassthrough {
			workload = cfg.defaultWorkload
		}
		if waitsForPartition && workload == consts.RBLNWorkloadConfigVMPassthrough {
			if rt.checkBindingDriver != nil {
				if err := rt.checkBindingDriver(ctx); err != nil {
					slog.Info("waiting for current driver installation", "err", err)
					rt.sleep(interval)
					continue
				}
			}
			if missing := statusfile.Missing(cfg.outputDir, []string{consts.PartitionReadyFileName}); len(missing) > 0 {
				slog.Info("waiting for partition manager", "node", cfg.nodeName)
				rt.sleep(interval)
				continue
			}
			result, err := rt.validateVFIO(vfiovalidator.Config{
				OutputDir:            cfg.outputDir,
				SleepIntervalSeconds: cfg.sleepIntervalSeconds,
			})
			if err == nil {
				slog.Info("gate passed", "component", cfg.component,
					"workload", consts.RBLNWorkloadConfigVMPassthrough, "boundDevices", result.BoundDevices)
				return nil
			}
			slog.Info("waiting for VFIO binding", "node", cfg.nodeName, "err", err)
			rt.sleep(interval)
			continue
		}

		if err := rt.checkDriver(ctx); err != nil {
			slog.Info("waiting for current driver container", "node", cfg.nodeName, "err", err)
			rt.sleep(interval)
			continue
		}

		readyFiles := []string{toolkitReadyFile}
		managerLabel := labels["rebellions.ai/npu.deploy.partition-manager"]
		managerEnabled := managerLabel == "true" || managerLabel == consts.RBLNDeployPausedForDriverUpgrade
		if waitsForPartition && (partitioned || managerEnabled) {
			readyFiles = append(readyFiles, consts.PartitionReadyFileName)
		}

		missing := statusfile.Missing(cfg.outputDir, readyFiles)
		if len(missing) == 0 {
			slog.Info("gate passed", "component", cfg.component, "partitioned", partitioned, "readyFiles", readyFiles)
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
