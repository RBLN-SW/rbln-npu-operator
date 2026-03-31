package driver

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
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
				"failed to validate the driver, retrying",
				"sleepSeconds",
				cfg.SleepIntervalSeconds,
				"err",
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
	slog.Info("validated driver library path", "path", driverLibraryPath)
	return nil
}

func assertDriverContainerReady(outputDir string, sleepIntervalSeconds int, silent bool) error {
	readyPath := filepath.Join(outputDir, driverContainerReadyFile)
	args := []string{"-c", fmt.Sprintf("stat %s", readyPath)}
	return runCommandWithWait(shell, args, sleepIntervalSeconds, silent)
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

func runCommand(name string, args []string, silent bool) error {
	cmd := exec.Command(name, args...)
	if !silent {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	return cmd.Run()
}

func runCommandWithWait(name string, args []string, sleepSeconds int, silent bool) error {
	for {
		err := runCommand(name, args, silent)
		if err == nil {
			return nil
		}
		slog.Info("command failed, retrying", "command", name, "err", err)
		time.Sleep(time.Duration(sleepSeconds) * time.Second)
	}
}
