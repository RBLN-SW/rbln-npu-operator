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
			"err", err, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("host driver not detected via %s: %w", hostSmiBinary, err)
	}

	version := strings.TrimSpace(string(out))
	if version == "" {
		return fmt.Errorf("host driver not detected: %s produced empty output", hostSmiBinary)
	}

	slog.Info("Host driver detected", "version", version)
	return nil
}
