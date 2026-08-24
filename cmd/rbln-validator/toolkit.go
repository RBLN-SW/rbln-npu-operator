package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
	toolkitvalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/toolkit"

	"github.com/spf13/cobra"
)

const (
	toolkitReadyFile = "toolkit-ready"
	cdiRootPath      = "/var/run/cdi"
)

type toolkitRuntime struct {
	validator toolkitvalidator.Validator
	cdiRoot   string
	sleep     func(time.Duration)
}

func defaultToolkitRuntime() toolkitRuntime {
	return toolkitRuntime{
		validator: toolkitvalidator.FSValidator{},
		cdiRoot:   cdiRootPath,
		sleep:     time.Sleep,
	}
}

func (r toolkitRuntime) checkReady(ctx context.Context) error {
	return r.validator.Validate(ctx, r.cdiRoot)
}

func newToolkitCommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "toolkit",
		Short: "Validate toolkit readiness",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateToolkit(cmd.Context(), config.toolkitConfig(), defaultToolkitRuntime())
		},
	}
}

func validateToolkit(ctx context.Context, cfg *toolkitConfig, rt toolkitRuntime) error {
	if err := statusfile.Prepare(cfg.outputDir, toolkitReadyFile); err != nil {
		return err
	}

	for {
		if err := rt.checkReady(ctx); err != nil {
			slog.Info("Toolkit is not ready", "err", err, "sleepSeconds", cfg.sleepIntervalSeconds)
			rt.sleep(time.Duration(cfg.sleepIntervalSeconds) * time.Second)
			continue
		}
		break
	}

	return statusfile.CreateEmpty(cfg.outputDir, toolkitReadyFile)
}
