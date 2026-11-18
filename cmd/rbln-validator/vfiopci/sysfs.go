package vfiopci

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	host "github.com/rbln-sw/rblnlib-go/pkg/host/v1"
	system "github.com/rbln-sw/rblnlib-go/pkg/system/v1"
)

var errDriverNotBound = errors.New("device has no driver symlink")

func hostClient(root string) (*host.Client, error) {
	return host.New(host.Options{PCIDevicesRoot: root})
}

func scanDevices(root string) ([]string, error) {
	c, err := hostClient(root)
	if err != nil {
		return nil, err
	}
	s, err := c.ListDevices(context.Background())
	if err != nil {
		return nil, err
	}
	if err := system.IssuesError(s.Issues); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(s.Devices))
	for _, d := range s.Devices {
		out = append(out, filepath.Join(root, d.PCIAddress))
	}
	return out, nil
}

func currentDriver(devPath string) (string, error) {
	c, err := hostClient(filepath.Dir(devPath))
	if err != nil {
		return "", err
	}
	return c.CurrentDriver(context.Background(), filepath.Base(devPath))
}

func verifyVFIOBinding(devPath string) error {
	driver, err := currentDriver(devPath)
	if err != nil {
		return err
	}
	if driver == "" {
		return fmt.Errorf("%s: %w", devPath, errDriverNotBound)
	}
	if driver != system.VFIODriver {
		return fmt.Errorf("device %s bound to %q, want %q", devPath, driver, system.VFIODriver)
	}
	return nil
}
