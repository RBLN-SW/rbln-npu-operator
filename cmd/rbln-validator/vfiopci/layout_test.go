package vfiopci

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeVFLayout(t *testing.T, counts []int) string {
	t.Helper()
	root := t.TempDir()
	for p, count := range counts {
		pf := fmt.Sprintf("0000:%02x:00.0", p+1)
		driver := "vfio-pci"
		if count > 0 {
			driver = "rebellions"
		}
		devices := []fakeDevice{{bdf: pf, vendor: rblnVendorID, class: rblnDeviceClassNPU, driver: driver}}
		for i := range count {
			devices = append(devices, fakeDevice{
				bdf: fmt.Sprintf("0000:%02x:01.%d", p+1, i), vendor: rblnVendorID,
				class: rblnDeviceClassNPU, driver: vfioPCIDriverName,
			})
		}
		writeFakeSysfs(t, root, devices)
		if err := os.WriteFile(filepath.Join(root, pf, "sriov_numvfs"), []byte(fmt.Sprint(count)), 0o600); err != nil {
			t.Fatal(err)
		}
		for i := range count {
			vf := devices[i+1].bdf
			if err := os.Symlink(filepath.Join(root, vf), filepath.Join(root, pf, fmt.Sprintf("virtfn%d", i))); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(root, pf), filepath.Join(root, vf, "physfn")); err != nil {
				t.Fatal(err)
			}
		}
	}
	return root
}

func TestVMNodeLayouts(t *testing.T) {
	for _, first := range []int{0, 1, 4} {
		for _, second := range []int{0, 1, 4} {
			t.Run(fmt.Sprintf("%d/%d", first, second), func(t *testing.T) {
				root := writeVFLayout(t, []int{first, second})
				result, err := Validate(Config{SysfsRoot: root})
				invalid := (first == 0) != (second == 0)
				if invalid {
					if err == nil {
						t.Fatal("accepted mixed PF/VF passthrough")
					}
					return
				}
				want := first + second
				if want == 0 {
					want = 2
				}
				if err != nil || len(result.BoundDevices) != want {
					t.Fatalf("valid layout: targets=%v err=%v, want %d", result.BoundDevices, err, want)
				}
			})
		}
	}
}

func TestIncompleteVFLayouts(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		value      *string
	}{
		{"unbound VF", "0000:01:01.0/driver", nil},
		{"missing parent link", "0000:01:01.0/physfn", nil},
		{"missing child link", "0000:01:00.0/virtfn0", nil},
		{"unbound hosting PF", "0000:01:00.0/driver", nil},
		{"count mismatch", "0000:01:00.0/sriov_numvfs", new("4")},
		{"unsupported count", "0000:01:00.0/sriov_numvfs", new("2")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeVFLayout(t, []int{1})
			path := filepath.Join(root, tc.path)
			var err error
			if tc.value == nil {
				err = os.Remove(path)
			} else {
				err = os.WriteFile(path, []byte(*tc.value), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Validate(Config{SysfsRoot: root}); err == nil {
				t.Fatal("accepted incomplete VF topology/binding")
			}
		})
	}
}
