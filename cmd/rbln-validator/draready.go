package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

// draReadyRuntime is the injectable surface of the dra-ready gate so the wait
// loop can be exercised without a host.
type draReadyRuntime struct {
	toolkitReady func(outputDir string) (bool, error)
	validateVFIO func(vfiovalidator.Config) (vfiovalidator.Result, error)
	sleep        func(context.Context, time.Duration) error
}

func defaultDRAReadyRuntime() draReadyRuntime {
	return draReadyRuntime{
		toolkitReady: toolkitReadyMarkerExists,
		validateVFIO: vfiovalidator.Validate,
		sleep:        sleepContext,
	}
}

func newDRAReadyCommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "dra-ready",
		Short: "Wait until this node can run the DRA kubelet plugin",
		Long: `Blocks until one of two node-local conditions holds:

  - container node: the operator-validator wrote <output-dir>/toolkit-ready,
    which implies the kernel driver is loaded, rbln-smi is installed and the
    RBLN CDI spec exists — everything the plugin needs to enumerate NPUs.
  - vm-passthrough node: every Rebellions NPU is bound to vfio-pci, which is
    what the plugin publishes there. toolkit-ready never appears on such a
    node because neither the validator nor the toolkit is deployed to it.

On any given node only one of the two can ever become true, so no node-type
lookup is needed. The command writes no status file.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			interval := time.Duration(config.sleepIntervalSeconds) * time.Second
			return waitDRAReady(cmd.Context(), config.outputDir, config.vfioPCIConfig(), interval, defaultDRAReadyRuntime())
		},
	}
}

func waitDRAReady(
	ctx context.Context,
	outputDir string,
	vfioCfg vfiovalidator.Config,
	interval time.Duration,
	rt draReadyRuntime,
) error {
	marker := filepath.Join(outputDir, toolkitReadyFile)
	for {
		ready, err := rt.toolkitReady(outputDir)
		if err != nil {
			return err
		}
		if ready {
			slog.Info("Container stack ready, DRA kubelet plugin may start", "marker", marker)
			return nil
		}

		result, vfioErr := rt.validateVFIO(vfioCfg)
		if vfioErr == nil {
			slog.Info("NPUs bound to vfio-pci, DRA kubelet plugin may start", "boundDevices", result.BoundDevices)
			return nil
		}

		// Both reasons on one line: an operator reading `kubectl logs -c dra-ready`
		// has to see which side of the gate this node is waiting on.
		slog.Info("DRA prerequisites not ready, retrying",
			"toolkitReadyMarker", marker,
			"vfioPCIError", vfioErr.Error(),
			"sleepSeconds", interval.Seconds())
		if err := rt.sleep(ctx, interval); err != nil {
			return err
		}
	}
}

func toolkitReadyMarkerExists(outputDir string) (bool, error) {
	marker := filepath.Join(outputDir, toolkitReadyFile)
	if _, err := os.Stat(marker); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", marker, err)
	}
	return true, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
