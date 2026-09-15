package main

import (
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

func newVFIOPCIAssertRBLNCommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "assert-rbln",
		Short: "Wait until every Rebellions NPU is bound to the rbln driver",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return assertRBLNBound(vfiovalidator.AssertConfig{}, defaultAssertRuntime(config.sleepIntervalSeconds))
		},
	}
}

// bindingWarnAfter is how long a dirty binding is treated as a driver install
// in progress before the retry line escalates to warn. It matches the driver
// container's startup-probe budget (PeriodSeconds × FailureThreshold in
// internal/driver/components/consts.go, 600 s): kubelet gives the module that
// long to load, so a binding still missing afterwards is not coming back on
// its own. Keep the two in step.
const bindingWarnAfter = 10 * time.Minute

type assertRuntime struct {
	assert    func(vfiovalidator.AssertConfig) (vfiovalidator.AssertResult, error)
	sleep     func(time.Duration)
	interval  time.Duration
	warnAfter time.Duration
}

func defaultAssertRuntime(sleepIntervalSeconds int) assertRuntime {
	return assertRuntime{
		assert:    vfiovalidator.AssertRBLNBound,
		sleep:     time.Sleep,
		interval:  time.Duration(sleepIntervalSeconds) * time.Second,
		warnAfter: bindingWarnAfter,
	}
}

// assertRBLNBound polls rather than failing: while k8s-driver-manager
// (re)installs the driver every NPU is transiently unbound, and a non-zero exit
// there only buys a kubelet restart with back-off (3–5 restarts and a BackOff
// Warning event per fresh install). Only a sysfs read error exits.
func assertRBLNBound(cfg vfiovalidator.AssertConfig, rt assertRuntime) error {
	var waited time.Duration
	for {
		result, err := rt.assert(cfg)
		if err != nil {
			return err
		}
		if len(result.DirtyDevices) == 0 {
			slog.Info("RBLN binding assertion", "clean", len(result.CleanDevices), "dirty", 0)
			return nil
		}
		dirty := make([]string, 0, len(result.DirtyDevices))
		for _, d := range result.DirtyDevices {
			driver := d.CurrentDriver
			if driver == "" {
				driver = "unbound"
			}
			dirty = append(dirty, d.BDF+"="+driver)
		}
		attrs := []any{
			"clean", len(result.CleanDevices),
			"dirty", len(result.DirtyDevices),
			"dirtyDevices", dirty,
			"waitedSeconds", waited.Seconds(),
			"sleepSeconds", rt.interval.Seconds(),
		}
		if waited >= rt.warnAfter {
			// Still polling — an init container has no timeout and a restart
			// would not help — but past the install window the only place this
			// state is visible is the log, so it must be alertable.
			slog.Warn("RBLN binding still not ready, retrying", attrs...)
		} else {
			slog.Info("RBLN binding not ready, retrying", attrs...)
		}
		rt.sleep(rt.interval)
		waited += rt.interval
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
			slog.Info("VFIO-PCI validation completed", "boundDevices", result.BoundDevices)
			return rt.writeStatus(cfg.OutputDir, result)
		}
		slog.Info("VFIO-PCI binding not ready, retrying",
			"error", err, "sleepSeconds", cfg.SleepIntervalSeconds)
		rt.sleep(time.Duration(cfg.SleepIntervalSeconds) * time.Second)
	}
}
