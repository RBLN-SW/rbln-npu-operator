package driver

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
)

func WriteStatusFile(outputDir string, result Result) error {
	content := strings.Join([]string{
		fmt.Sprintf("IS_HOST_DRIVER=%t", result.IsHostDriver),
		fmt.Sprintf("RBLN_CTK_DAEMON_HOST_ROOT=%s", result.DriverRootCtrPath),
		fmt.Sprintf("RBLN_CTK_DAEMON_DRIVER_ROOT=%s", result.DriverRoot),
		fmt.Sprintf("RBLN_CTK_DAEMON_CONTAINER_LIBRARY_PATH=%s", result.ContainerLibraryPath),
	}, "\n") + "\n"

	return statusfile.CreateWithContent(filepath.Join(outputDir, driverReadyFile), content)
}
