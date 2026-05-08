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
