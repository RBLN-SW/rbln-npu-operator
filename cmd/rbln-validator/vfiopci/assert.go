package vfiopci

import (
	"fmt"
	"path/filepath"
)

type AssertConfig struct {
	SysfsRoot      string
	ExpectedDriver string
}

type DirtyDevice struct {
	BDF           string
	CurrentDriver string
}

type AssertResult struct {
	CleanDevices []string
	DirtyDevices []DirtyDevice
}

var currentDriverFn = currentDriver

func AssertRBLNBound(cfg AssertConfig) (AssertResult, error) {
	root := cfg.SysfsRoot
	if root == "" {
		root = pciDevicesRootDefault
	}
	expected := cfg.ExpectedDriver
	if expected == "" {
		expected = rblnDriverNameDefault
	}

	matched, err := scanDevicesFn(root)
	if err != nil {
		return AssertResult{}, fmt.Errorf("scan pci devices under %s: %w", root, err)
	}

	result := AssertResult{
		CleanDevices: make([]string, 0, len(matched)),
		DirtyDevices: make([]DirtyDevice, 0),
	}
	for _, devPath := range matched {
		driver, err := currentDriverFn(devPath)
		if err != nil {
			return AssertResult{}, fmt.Errorf("read driver for %s: %w", devPath, err)
		}
		bdf := filepath.Base(devPath)
		if driver == expected {
			result.CleanDevices = append(result.CleanDevices, bdf)
			continue
		}
		result.DirtyDevices = append(result.DirtyDevices, DirtyDevice{
			BDF:           bdf,
			CurrentDriver: driver,
		})
	}
	return result, nil
}
