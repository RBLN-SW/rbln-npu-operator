package driver

import (
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
)

func validateHostDriver() error {
	slog.Info("Attempting to validate a pre-installed driver on the host")
	installed, err := detectInstalledHostDriver()
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf("host driver module %q not installed", hostDriverModuleName)
	}
	loaded, err := detectLoadedHostDriver()
	if err != nil {
		return err
	}
	if !loaded {
		return fmt.Errorf("host driver module %q not loaded", hostDriverModuleName)
	}
	return nil
}

func detectInstalledHostDriver() (bool, error) {
	versionCmd := exec.Command(
		"chroot",
		hostRootMountPath,
		"modinfo",
		"-F",
		"version",
		hostDriverModuleName,
	)
	versionOut, err := versionCmd.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return false, nil
		}
		slog.Debug("host driver version check failed", "err", err)
		return false, err
	}
	version := strings.TrimSpace(string(versionOut))
	if version != "" {
		slog.Info("Detected host driver module version", "module", hostDriverModuleName, "version", version)
	}
	return version != "", nil
}

func detectLoadedHostDriver() (bool, error) {
	loadedCmd := exec.Command(
		"chroot",
		hostRootMountPath,
		"test",
		"-d",
		"/sys/module/"+hostDriverModuleName,
	)
	if err := loadedCmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return false, nil
		}
		slog.Debug("host driver load check failed", "err", err)
		return false, err
	}
	slog.Info("Detected host driver module loaded", "module", hostDriverModuleName)
	return true, nil
}
