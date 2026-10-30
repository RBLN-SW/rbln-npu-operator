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

package toolkit

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type FSValidator struct{}

func (FSValidator) Validate(_ context.Context, cdiRootPath string) error {
	err := findNonEmptyRBLNCDISpec(cdiRootPath)
	if err != nil {
		return err
	}
	return nil
}

func findNonEmptyRBLNCDISpec(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.Contains(strings.ToLower(name), "rbln") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.Size() == 0 {
			continue
		}

		return nil
	}

	return fmt.Errorf("no non-empty rbln cdi spec found in %s", dir)
}
