package main

import (
	"log/slog"

	drivervalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/driver"
	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"

	"github.com/spf13/cobra"
)

func newDriverCommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "driver",
		Short: "Validate driver readiness",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateDriver(config.driverConfig(), defaultDriverRuntime())
		},
	}
}

type driverRuntime struct {
	validate    func(drivervalidator.Config) (drivervalidator.Result, error)
	writeStatus func(string, drivervalidator.Result) error
}

func defaultDriverRuntime() driverRuntime {
	return driverRuntime{
		validate:    drivervalidator.Validate,
		writeStatus: drivervalidator.WriteStatusFile,
	}
}

func validateDriver(cfg drivervalidator.Config, rt driverRuntime) error {
	if err := statusfile.Prepare(cfg.OutputDir, drivervalidator.ReadyFileName); err != nil {
		return err
	}

	info, err := rt.validate(cfg)
	if err != nil {
		slog.Error("Driver is not ready", "err", err)
		return err
	}
	slog.Info("Driver validation completed", "hostDriver", info.IsHostDriver)

	return rt.writeStatus(cfg.OutputDir, info)
}
