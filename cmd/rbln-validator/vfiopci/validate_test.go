package vfiopci

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeDevice struct {
	bdf    string
	vendor string
	class  string
	driver string // driver name (basename) to symlink; empty means no driver symlink
}

func writeFakeSysfs(t *testing.T, root string, devices []fakeDevice) {
	t.Helper()

	driversRoot := filepath.Join(root, "..", "drivers")
	if err := os.MkdirAll(driversRoot, 0o750); err != nil {
		t.Fatalf("mkdir drivers: %v", err)
	}

	for _, d := range devices {
		devPath := filepath.Join(root, d.bdf)
		if err := os.MkdirAll(devPath, 0o750); err != nil {
			t.Fatalf("mkdir dev %s: %v", d.bdf, err)
		}
		if err := os.WriteFile(filepath.Join(devPath, "vendor"), []byte(d.vendor+"\n"), 0o600); err != nil {
			t.Fatalf("write vendor: %v", err)
		}
		if err := os.WriteFile(filepath.Join(devPath, "class"), []byte(d.class+"\n"), 0o600); err != nil {
			t.Fatalf("write class: %v", err)
		}
		if d.driver == "" {
			continue
		}
		driverPath := filepath.Join(driversRoot, d.driver)
		if err := os.MkdirAll(driverPath, 0o750); err != nil {
			t.Fatalf("mkdir driver %s: %v", d.driver, err)
		}
		if err := os.Symlink(driverPath, filepath.Join(devPath, "driver")); err != nil {
			t.Fatalf("symlink driver: %v", err)
		}
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]struct {
		reason       string
		devices      []fakeDevice
		wantErr      bool
		wantErrSub   string
		wantBoundLen int
	}{
		"NoNPUDevices": {
			reason:     "labeled sandbox node with zero matching devices is a misconfig",
			devices:    nil,
			wantErr:    true,
			wantErrSub: "no rebellions npu devices",
		},
		"AllBoundToVFIO": {
			reason: "all NPU devices bound to vfio-pci returns success",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
			},
			wantErr:      false,
			wantBoundLen: 2,
		},
		"OneDeviceBoundToWrongDriver": {
			reason: "any device bound to non-vfio-pci driver fails",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000", driver: "rebellions"},
			},
			wantErr:    true,
			wantErrSub: "not bound to vfio-pci",
		},
		"OneDeviceUnbound": {
			reason: "missing driver symlink fails",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci"},
				{bdf: "0000:02:00.0", vendor: "0x1eff", class: "0x120000"}, // no driver
			},
			wantErr:    true,
			wantErrSub: "not bound to vfio-pci",
		},
		"WrongVendorIgnored": {
			reason: "non-rebellions vendors are filtered out and the remaining count drives the result",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x10de", class: "0x120000", driver: "vfio-pci"},
			},
			wantErr:    true,
			wantErrSub: "no rebellions npu devices",
		},
		"WrongClassIgnored": {
			reason: "non-NPU class filtered out",
			devices: []fakeDevice{
				{bdf: "0000:01:00.0", vendor: "0x1eff", class: "0x030000", driver: "vfio-pci"},
			},
			wantErr:    true,
			wantErrSub: "no rebellions npu devices",
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

			result, err := Validate(Config{SysfsRoot: devicesRoot})

			if tc.wantErr {
				if err == nil {
					t.Fatalf("%s: expected error, got nil", tc.reason)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("%s: error %q does not contain %q", tc.reason, err.Error(), tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if len(result.BoundDevices) != tc.wantBoundLen {
				t.Fatalf("%s: BoundDevices len = %d, want %d", tc.reason, len(result.BoundDevices), tc.wantBoundLen)
			}
		})
	}
}
