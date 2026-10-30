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
		slog.Error("Driver is not ready", "error", err)
		return err
	}
	slog.Info("Driver validation completed", "hostDriver", info.IsHostDriver)

	return rt.writeStatus(cfg.OutputDir, info)
}
