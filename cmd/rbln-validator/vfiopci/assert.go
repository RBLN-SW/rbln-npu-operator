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
