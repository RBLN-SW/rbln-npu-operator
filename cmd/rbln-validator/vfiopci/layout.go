package vfiopci

import (
	"context"
	"fmt"
	"path/filepath"

	system "github.com/rbln-sw/rblnlib-go/pkg/system/v1"
)

// The SDK validates hardware relationships; workload-mode policy remains here.
func passthroughDevices(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no passthrough devices in PCI topology")
	}
	root := filepath.Dir(paths[0])
	c, err := hostClient(root)
	if err != nil {
		return nil, err
	}
	devices, err := c.ObservePartitions(context.Background())
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, p := range paths {
		selected[filepath.Base(p)] = true
	}
	var targets []string
	hasPF, hasVF := false, false
	for _, d := range devices {
		if !selected[d.PCIAddress] {
			continue
		}
		if d.FunctionKind == system.VirtualFunction {
			hasVF = true
			targets = append(targets, filepath.Join(root, d.PCIAddress))
			continue
		}
		count := uint32(0)
		if d.SRIOV != nil {
			count = d.SRIOV.EnabledVFs
		}
		if count != 0 && count != 1 && count != 4 {
			return nil, fmt.Errorf("PF %s has unsupported VF count %d", d.PCIAddress, count)
		}
		if count == 0 {
			hasPF = true
			targets = append(targets, filepath.Join(root, d.PCIAddress))
			continue
		}
		if d.Driver != system.NativeDriver {
			return nil, fmt.Errorf("VF-hosting PF %s must be bound to rebellions, got %q", d.PCIAddress, d.Driver)
		}
		for _, vf := range d.SRIOV.VirtualFunctions {
			if !selected[vf] {
				return nil, fmt.Errorf("VF %s missing from selection", vf)
			}
		}
	}
	if hasPF && hasVF {
		return nil, fmt.Errorf("vm-passthrough requires PF-only or VF-only per node; PF/VF passthrough may not coexist")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no passthrough devices in PCI topology")
	}
	return targets, nil
}
