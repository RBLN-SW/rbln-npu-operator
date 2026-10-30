/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/rebellions-sw/rbln-npu-operator/internal/driverready"
)

func newDriverReadyCommand(config *rootConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "driver-ready",
		Short: "Wait for this node's running driver container and its readiness marker",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.driverConfig()
			checker, err := driverready.New(cfg.NodeName, cfg.Namespace, nil)
			if err != nil {
				return err
			}
			return waitForCurrentDriver(cmd.Context(), cfg.OutputDir,
				time.Duration(cfg.SleepIntervalSeconds)*time.Second, checker.Check)
		},
	}
}

func waitForCurrentDriver(
	ctx context.Context, outputDir string, interval time.Duration, check func(context.Context) error,
) error {
	for {
		err := check(ctx)
		if err == nil {
			_, err = os.Stat(filepath.Join(outputDir, ".driver-ctr-ready"))
		}
		if err == nil {
			return nil
		}
		slog.Info("Waiting for current driver container", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
