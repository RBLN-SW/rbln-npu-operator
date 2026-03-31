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
