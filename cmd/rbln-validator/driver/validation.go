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

package driver

import "log/slog"

type Config struct {
	OutputDir            string
	SleepIntervalSeconds int
	NodeName             string
	Namespace            string
}

type Result struct {
	IsHostDriver         bool
	DriverRoot           string
	DriverRootCtrPath    string
	ContainerLibraryPath string
}

var (
	validateHostDriverFn      = validateHostDriver
	validateDriverContainerFn = validateDriverContainer
)

func Validate(cfg Config) (Result, error) {
	return validate(cfg, false)
}

func validate(cfg Config, silent bool) (Result, error) {
	if err := validateHostDriverFn(); err == nil {
		slog.Info("Detected a pre-installed driver on the host")
		return Result{
			IsHostDriver:         true,
			DriverRoot:           "/",
			DriverRootCtrPath:    "/host",
			ContainerLibraryPath: driverContainerLibraryPath,
		}, nil
	}

	if err := validateDriverContainerFn(cfg, silent); err != nil {
		return Result{}, err
	}

	return Result{
		IsHostDriver:         false,
		DriverRoot:           driverInstallDirDefault,
		DriverRootCtrPath:    "/host",
		ContainerLibraryPath: driverContainerLibraryPath,
	}, nil
}
