package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

func newVFIOPCICommand(config *rootConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vfio-pci",
		Short: "Validate that NPU devices are bound to vfio-pci",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateVFIOPCI(config.vfioPCIConfig(), defaultVFIOPCIRuntime())
		},
	}
	cmd.AddCommand(newVFIOPCIAssertRBLNCommand(config))
	return cmd
}

func newVFIOPCIAssertRBLNCommand(_ *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "assert-rbln",
		Short: "Assert that every Rebellions NPU is bound to the rbln driver",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return assertRBLNBound(vfiovalidator.AssertConfig{})
		},
	}
}

func assertRBLNBound(cfg vfiovalidator.AssertConfig) error {
	result, err := vfiovalidator.AssertRBLNBound(cfg)
	if err != nil {
		return err
	}
	slog.Info("RBLN binding assertion",
		"clean", len(result.CleanDevices),
		"dirty", len(result.DirtyDevices))
	if len(result.DirtyDevices) > 0 {
		for _, d := range result.DirtyDevices {
			slog.Error("Device not bound to rbln driver",
				"bdf", d.BDF,
				"currentDriver", d.CurrentDriver)
		}
		return fmt.Errorf("%d device(s) not bound to rbln driver", len(result.DirtyDevices))
	}
	return nil
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
			slog.Info("VFIO-PCI validation completed", "boundDevices", result.BoundDevices)
			return rt.writeStatus(cfg.OutputDir, result)
		}
		slog.Info("VFIO-PCI binding not ready, retrying",
			"error", err, "sleepSeconds", cfg.SleepIntervalSeconds)
		rt.sleep(time.Duration(cfg.SleepIntervalSeconds) * time.Second)
	}
}
