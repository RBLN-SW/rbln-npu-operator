package driver

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	if err := assertDriverContainerReady(cfg.OutputDir, cfg.SleepIntervalSeconds, silent); err != nil {
		return fmt.Errorf("error checking driver container status: %w", err)
	}

	for {
		slog.Info("Attempting to validate a driver container installation")
		if err := validateDriverInstall(driverInstallDirDefault); err != nil {
			slog.Info(
				"Failed to validate the driver, retrying",
				"sleepSeconds",
				cfg.SleepIntervalSeconds,
				"error",
				err,
			)
			time.Sleep(time.Duration(cfg.SleepIntervalSeconds) * time.Second)
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
