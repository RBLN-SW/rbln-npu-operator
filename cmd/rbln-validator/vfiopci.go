package main

import (
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

func newVFIOPCICommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "vfio-pci",
		Short: "Validate that NPU devices are bound to vfio-pci",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateVFIOPCI(config.vfioPCIConfig(), defaultVFIOPCIRuntime())
		},
	}
}

type vfioPCIRuntime struct {
	validate    func(vfiovalidator.Config) (vfiovalidator.Result, error)
	writeStatus func(string, vfiovalidator.Result) error
	sleep       func(time.Duration)
}

func defaultVFIOPCIRuntime() vfioPCIRuntime {
	return vfioPCIRuntime{
		validate:    vfiovalidator.Validate,
		writeStatus: vfiovalidator.WriteStatusFile,
		sleep:       time.Sleep,
	}
}

func validateVFIOPCI(cfg vfiovalidator.Config, rt vfioPCIRuntime) error {
	if err := statusfile.Prepare(cfg.OutputDir, vfiovalidator.ReadyFileName); err != nil {
		return err
	}

	for {
		result, err := rt.validate(cfg)
		if err == nil {
			slog.Info("vfio-pci validation completed", "boundDevices", result.BoundDevices)
			return rt.writeStatus(cfg.OutputDir, result)
		}
		slog.Info("vfio-pci binding not ready, retrying",
			"err", err, "sleepSeconds", cfg.SleepIntervalSeconds)
		rt.sleep(time.Duration(cfg.SleepIntervalSeconds) * time.Second)
	}
}
