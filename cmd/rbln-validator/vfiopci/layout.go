package vfiopci

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// passthroughDevices validates a complete node layout and returns only the
// devices handed to VMs. Native PFs hosting VFs are not passthrough devices.
func passthroughDevices(devices []string) ([]string, error) {
	byBDF := make(map[string]string, len(devices))
	parents := make(map[string]string)
	for _, dev := range devices {
		byBDF[filepath.Base(dev)] = dev
	}
	for _, dev := range devices {
		parent, err := os.Readlink(filepath.Join(dev, "physfn"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read %s/physfn: %w", dev, err)
		}
		parentBDF := filepath.Base(parent)
		if _, ok := byBDF[parentBDF]; !ok {
			return nil, fmt.Errorf("VF %s has missing parent %s", dev, parentBDF)
		}
		parents[filepath.Base(dev)] = parentBDF
	}
	var targets []string
	seenVFs := make(map[string]bool)
	hasPF, hasVF := false, false
	for _, dev := range devices {
		bdf := filepath.Base(dev)
		if _, vf := parents[bdf]; vf {
			continue
		}
		count := 0
		value, err := readSysfsField(dev, "sriov_numvfs")
		if err == nil {
			count, err = strconv.Atoi(value)
			if err != nil || (count != 0 && count != 1 && count != 4) {
				return nil, fmt.Errorf("PF %s has unsupported sriov_numvfs %q", dev, value)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read %s/sriov_numvfs: %w", dev, err)
		}
		links, err := filepath.Glob(filepath.Join(dev, "virtfn*"))
		if err != nil {
			return nil, err
		}
		if len(links) != count {
			return nil, fmt.Errorf("PF %s reports %d VFs but has %d virtfn links", dev, count, len(links))
		}
		if count == 0 {
			hasPF = true
			targets = append(targets, dev)
			continue
		}
		hasVF = true
		driver, err := currentDriver(dev)
		if err != nil {
			return nil, err
		}
		if driver != "rebellions" {
			return nil, fmt.Errorf("VF-hosting PF %s must be bound to rebellions, got %q", dev, driver)
		}
		for i := range count {
			target, err := os.Readlink(filepath.Join(dev, fmt.Sprintf("virtfn%d", i)))
			if err != nil {
				return nil, fmt.Errorf("read VF %d of %s: %w", i, dev, err)
			}
			vfBDF := filepath.Base(target)
			vf, exists := byBDF[vfBDF]
			if !exists || parents[vfBDF] != bdf || seenVFs[vfBDF] {
				return nil, fmt.Errorf("PF %s has inconsistent VF link to %s", dev, vfBDF)
			}
			seenVFs[vfBDF] = true
			targets = append(targets, vf)
		}
	}
	if len(seenVFs) != len(parents) {
		return nil, fmt.Errorf("incomplete VF topology: %d of %d VFs linked by a PF", len(seenVFs), len(parents))
	}
	if hasPF && hasVF {
		return nil, fmt.Errorf("vm-passthrough requires PF-only or VF-only per node; PF/VF passthrough may not coexist")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no passthrough devices in PCI topology")
	}
	return targets, nil
}
