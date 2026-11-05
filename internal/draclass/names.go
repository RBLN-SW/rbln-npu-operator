// Package draclass defines the DeviceClass names published by the operator.
package draclass

import (
	"fmt"
	"strings"
)

func VFName(driver string, count int) string {
	name, domain, hasDomain := strings.Cut(driver, ".")
	name = fmt.Sprintf("%s-vf%d", name, count)
	if hasDomain {
		name += "." + domain
	}
	return name
}

func VFIOName(nativeClass string) string { return "vfio-" + nativeClass }

// Names includes pending PF/VF claims even when the API server has pruned the
// extended-resource bridge or the DeviceClass list is temporarily unavailable.
func Names(driver string) []string {
	return []string{
		driver, VFIOName(driver),
		VFName(driver, 1), VFName(driver, 4),
		VFIOName(VFName(driver, 1)), VFIOName(VFName(driver, 4)),
	}
}
