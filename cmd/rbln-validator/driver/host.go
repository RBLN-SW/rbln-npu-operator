package driver

import (
	"context"
	"log/slog"
	"time"

	host "github.com/rbln-sw/rblnlib-go/pkg/host/v1"
)

const hostDriverProbeTimeout = 10 * time.Second

func validateHostDriver() error {
	c, err := host.New(host.Options{HostPID: 1, CommandTimeout: hostDriverProbeTimeout})
	if err != nil {
		return err
	}
	version, err := c.ProbeHostDriver(context.Background())
	if err != nil {
		return err
	}
	slog.Info("Host driver detected", "version", version)
	return nil
}
