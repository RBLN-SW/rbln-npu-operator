package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
)

// HostDriverValidated reads the result of the host probe already performed
// by driver validation. Missing Pod ownership alone cannot prove a host install.
func HostDriverValidated(outputDir string) (bool, error) {
	// #nosec G304 -- path is derived from the validator-managed output directory.
	data, err := os.ReadFile(filepath.Join(outputDir, driverReadyFile))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read driver readiness record: %w", err)
	}
	values := make(map[string]string, 2)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return false, fmt.Errorf("invalid driver readiness record")
		}
		if key != "IS_HOST_DRIVER" && key != "RBLN_CTK_DAEMON_DRIVER_ROOT" {
			continue
		}
		if _, duplicate := values[key]; duplicate {
			return false, fmt.Errorf("duplicate %s in driver readiness record", key)
		}
		values[key] = value
	}
	return values["IS_HOST_DRIVER"] == "true" && values["RBLN_CTK_DAEMON_DRIVER_ROOT"] == "/", nil
}

func WriteStatusFile(outputDir string, result Result) error {
	content := strings.Join([]string{
		fmt.Sprintf("IS_HOST_DRIVER=%t", result.IsHostDriver),
		fmt.Sprintf("RBLN_CTK_DAEMON_HOST_ROOT=%s", result.DriverRootCtrPath),
		fmt.Sprintf("RBLN_CTK_DAEMON_DRIVER_ROOT=%s", result.DriverRoot),
		fmt.Sprintf("RBLN_CTK_DAEMON_CONTAINER_LIBRARY_PATH=%s", result.ContainerLibraryPath),
	}, "\n") + "\n"

	return statusfile.CreateWithContent(filepath.Join(outputDir, driverReadyFile), content)
}
