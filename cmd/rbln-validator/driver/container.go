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

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rebellions-sw/rbln-npu-operator/internal/driverready"
)

var driverLibrarySearchPaths = []string{
	"/usr/lib",
	"/usr/lib64",
	"/usr/lib/x86_64-linux-gnu",
	"/usr/lib/aarch64-linux-gnu",
	"/lib64",
	"/lib/x86_64-linux-gnu",
	"/lib/aarch64-linux-gnu",
}

func validateDriverContainer(cfg Config, silent bool) error {
	checker, err := driverready.New(cfg.NodeName, cfg.Namespace, nil)
	if err != nil {
		return err
	}
	if err := assertDriverContainerReady(cfg.OutputDir, cfg.SleepIntervalSeconds, silent); err != nil {
		return fmt.Errorf("error checking driver container status: %w", err)
	}

	return waitForDriverInstall(cfg, func() error { return checker.Check(context.Background()) },
		// The host-root parent observes rootfs replacements; a separate child
		// hostPath can keep exposing the old filesystem after it is unmounted.
		func() error { return validateDriverInstall(hostRootMountPath + driverInstallDirDefault) }, time.Sleep)
}

func waitForDriverInstall(cfg Config, checkReady, validateInstall func() error, sleep func(time.Duration)) error {
	for {
		slog.Info("Attempting to validate a driver container installation")
		err := checkReady()
		if err == nil {
			_, err = os.Stat(filepath.Join(cfg.OutputDir, driverContainerReadyFile))
		}
		if err == nil {
			err = validateInstall()
		}
		if err == nil {
			// A library check must not carry readiness across a container restart.
			err = checkReady()
		}
		if err != nil {
			slog.Info(
				"Failed to validate the driver, retrying",
				"sleepSeconds",
				cfg.SleepIntervalSeconds,
				"error",
				err,
			)
			sleep(time.Duration(cfg.SleepIntervalSeconds) * time.Second)
			continue
		}
		return nil
	}
}

func validateDriverInstall(root string) error {
	driverLibraryPath, err := findDriverLibraryPath(root)
	if err != nil {
		return fmt.Errorf("failed to locate driver libraries: %w", err)
	}
	slog.Info("Validated driver library path", "path", driverLibraryPath)
	return nil
}

func assertDriverContainerReady(outputDir string, sleepIntervalSeconds int, silent bool) error {
	marker := filepath.Join(outputDir, driverContainerReadyFile)
	return waitForFile(marker, time.Duration(sleepIntervalSeconds)*time.Second, time.Sleep, silent)
}

// waitForFile polls until path exists, naming what it waits on every
// interval like the vfio-pci, dra-ready and toolkit gates do, so the tail of
// `kubectl logs -c driver-validation` always says where the pod is blocked.
func waitForFile(path string, interval time.Duration, sleep func(time.Duration), silent bool) error {
	for {
		_, err := os.Stat(path)
		if err == nil {
			if !silent {
				slog.Info("Driver container ready marker found", "path", path)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("driver container ready marker: %w", err)
		}
		if !silent {
			slog.Info("Driver container ready marker not found, retrying", "path", path, "sleepSeconds", interval.Seconds())
		}
		sleep(interval)
	}
}

func findDriverLibraryPath(root string) (string, error) {
	return findFileUnderRoot(root, "librbln-ml.so", driverLibrarySearchPaths...)
}

func findFileUnderRoot(root string, name string, searchIn ...string) (string, error) {
	paths := make([]string, 0, len(searchIn)+1)
	paths = append(paths, "")
	paths = append(paths, searchIn...)

	for _, dir := range paths {
		relative := strings.TrimPrefix(dir, "/")
		candidate := filepath.Join(root, relative, name)

		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		return resolved, nil
	}

	return "", fmt.Errorf("error locating %q under %q", name, root)
}
