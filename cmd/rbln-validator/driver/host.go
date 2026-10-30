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
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

const hostDriverProbeTimeout = 10 * time.Second

func validateHostDriver() error {
	slog.Info("Attempting to validate a pre-installed driver on the host")

	ctx, cancel := context.WithTimeout(context.Background(), hostDriverProbeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"nsenter", "--target", "1", "--mount", "--uts", "--ipc",
		"--", "env", "TMPDIR=/tmp", hostSmiBinary, "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.Info("RBLN-SMI execution failed, host driver not detected",
			"error", err, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("host driver not detected via %s: %w", hostSmiBinary, err)
	}

	version := strings.TrimSpace(string(out))
	if version == "" {
		return fmt.Errorf("host driver not detected: %s produced empty output", hostSmiBinary)
	}

	slog.Info("Host driver detected", "version", version)
	return nil
}
