package vfiopci

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestAssertRBLNBound(t *testing.T) {
	cases := map[string]struct {
		reason         string
		devices        []fakeDevice
		wantClean      []string
		wantDirty      map[string]string
		expectedDriver string
	}{
		"NoRebellionsDevicesPresent": {
			reason:    "fresh node never bound to vfio-pci has no rebellions devices to assert; trivially clean",
			devices:   nil,
			wantClean: []string{},
			wantDirty: map[string]string{},
		},
		"AllBoundToRebellions": {
			reason: "every device on rebellions driver is the expected post-cleanup state",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
			},
			wantClean: []string{"0000:01:00.0", "0000:02:00.0"},
			wantDirty: map[string]string{},
		},
		"OneDeviceStillOnVFIO": {
			reason: "a device left on vfio-pci after PreStop cleanup must surface as dirty",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
			},
			wantClean: []string{"0000:01:00.0"},
			wantDirty: map[string]string{"0000:02:00.0": "vfio-pci"},
		},
		"OneDeviceUnbound": {
			reason: "a device with no driver symlink (drivers_probe failed) is dirty with empty driver",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000"},
			},
			wantClean: []string{"0000:01:00.0"},
			wantDirty: map[string]string{"0000:02:00.0": ""},
		},
		"MixedCleanAndDirty": {
			reason: "multi-device dirty set is reported in full so operator-validator surfaces every bad BDF",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
				{bdf: "0000:03:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
				{bdf: "0000:04:00.0", vendor: "0x1eff", class: "0x120000"},
			},
			wantClean: []string{"0000:01:00.0", "0000:03:00.0"},
			wantDirty: map[string]string{
				"0000:02:00.0": "vfio-pci",
				"0000:04:00.0": "",
			},
		},
		"NonRebellionsVendorIgnored": {
			reason: "other vendors don't pollute the assertion (we only own 0x1eff)",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x10de", class: "0x120000", driver: "vfio-pci"},
			},
			wantClean: []string{},
			wantDirty: map[string]string{},
		},
		"CustomExpectedDriver": {
			reason: "expectedDriver override lets ops accept an alternate driver name",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "rbln"},
			},
			wantClean:      []string{"0000:01:00.0"},
			wantDirty:      map[string]string{},
			expectedDriver: "rbln",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tmp := t.TempDir()
			devicesRoot := filepath.Join(tmp, "devices")
			if err := os.MkdirAll(devicesRoot, 0o750); err != nil {
				t.Fatalf("mkdir devices: %v", err)
			}
			writeFakeSysfs(t, devicesRoot, tc.devices)

			result, err := AssertRBLNBound(AssertConfig{
				SysfsRoot:      devicesRoot,
				ExpectedDriver: tc.expectedDriver,
			})
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}

			sort.Strings(result.CleanDevices)
			sort.Strings(tc.wantClean)
			if !equalStringSlice(result.CleanDevices, tc.wantClean) {
				t.Errorf("%s: clean = %v, want %v", tc.reason, result.CleanDevices, tc.wantClean)
			}

			gotDirty := make(map[string]string, len(result.DirtyDevices))
			for _, d := range result.DirtyDevices {
				gotDirty[d.BDF] = d.CurrentDriver
			}
			if len(gotDirty) != len(tc.wantDirty) {
				t.Fatalf("%s: dirty = %v, want %v", tc.reason, gotDirty, tc.wantDirty)
			}
			for bdf, wantDriver := range tc.wantDirty {
				gotDriver, ok := gotDirty[bdf]
				if !ok {
					t.Errorf("%s: expected dirty BDF %q missing", tc.reason, bdf)
					continue
				}
				if gotDriver != wantDriver {
					t.Errorf("%s: dirty %s driver = %q, want %q", tc.reason, bdf, gotDriver, wantDriver)
				}
			}
		})
	}
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
