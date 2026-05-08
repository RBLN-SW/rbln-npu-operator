package vfiopci

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errDriverNotBound = errors.New("device has no driver symlink")

func scanDevices(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}

	matched := make([]string, 0, len(entries))
	for _, entry := range entries {
		devPath := filepath.Join(root, entry.Name())

		vendor, vendorErr := readSysfsField(devPath, "vendor")
		if vendorErr != nil || vendor != rblnVendorID {
			continue
		}
		class, classErr := readSysfsField(devPath, "class")
		if classErr != nil || class != rblnDeviceClassNPU {
			continue
		}
		matched = append(matched, devPath)
	}
	return matched, nil
}

func verifyVFIOBinding(devPath string) error {
	target, err := os.Readlink(filepath.Join(devPath, "driver"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", devPath, errDriverNotBound)
		}
		return fmt.Errorf("readlink %s/driver: %w", devPath, err)
	}
	driverName := filepath.Base(target)
	if driverName != vfioPCIDriverName {
		return fmt.Errorf("device %s bound to %q, want %q", devPath, driverName, vfioPCIDriverName)
	}
	return nil
}

func readSysfsField(devPath, field string) (string, error) {
	// #nosec G304 -- devPath comes from os.ReadDir under the sysfs root and field is a package-internal constant.
	data, err := os.ReadFile(filepath.Join(devPath, field))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
