package vfiopci

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/statusfile"
)

func WriteStatusFile(outputDir string, result Result) error {
	bdfs := make([]string, len(result.BoundDevices))
	for i, dev := range result.BoundDevices {
		bdfs[i] = filepath.Base(dev)
	}
	content := fmt.Sprintf("VFIO_PCI_BOUND_DEVICES=%s\n", strings.Join(bdfs, ","))
	return statusfile.CreateWithContent(filepath.Join(outputDir, ReadyFileName), content)
}
