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
