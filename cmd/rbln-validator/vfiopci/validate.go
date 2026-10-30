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

import "fmt"

type Config struct {
	OutputDir            string
	SleepIntervalSeconds int
	SysfsRoot            string
}

type Result struct {
	BoundDevices []string
}

var (
	scanDevicesFn       = scanDevices
	verifyVFIOBindingFn = verifyVFIOBinding
)

func Validate(cfg Config) (Result, error) {
	root := cfg.SysfsRoot
	if root == "" {
		root = pciDevicesRootDefault
	}

	matched, err := scanDevicesFn(root)
	if err != nil {
		return Result{}, fmt.Errorf("scan pci devices under %s: %w", root, err)
	}
	if len(matched) == 0 {
		return Result{}, fmt.Errorf("no rebellions npu devices (vendor %s class %s) found under %s",
			rblnVendorID, rblnDeviceClassNPU, root)
	}

	unbound := make([]string, 0, len(matched))
	for _, dev := range matched {
		if bindErr := verifyVFIOBindingFn(dev); bindErr != nil {
			unbound = append(unbound, dev)
		}
	}
	if len(unbound) > 0 {
		return Result{}, fmt.Errorf("npu devices not bound to %s: %v", vfioPCIDriverName, unbound)
	}

	return Result{BoundDevices: matched}, nil
}
