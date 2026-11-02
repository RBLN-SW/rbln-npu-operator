package components

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func renderedVFIOScript(t *testing.T) string {
	t.Helper()
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newTestOwner()
	owner.Spec.VFIOManager.Enabled = true
	p := NewVFIOManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: testNamespace, Name: consts.RBLNBaseName + "-" + consts.RBLNVFIOManagerName + "-config"}
	if err := c.Get(context.Background(), key, cm); err != nil {
		t.Fatal(err)
	}
	return cm.Data["vfio-manage.sh"]
}

func TestVFIOScriptCompatibility(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is required for the VFIO script tests")
	}
	script := renderedVFIOScript(t)
	const addr = "0000:01:00.0"
	for _, tc := range []struct {
		name, command, driver, vendor, class, output string
		exit                                         int
		bind                                         bool
	}{
		{name: "empty-bind", command: "bind", exit: 1},
		{name: "empty-check", command: "check_bind", output: "No Rebellions NPU devices found."},
		{name: "unbound-bind", command: "bind", vendor: "0x1eff", class: "0x120000", bind: true, output: "binding device " + addr},
		{name: "unbound-check", command: "check_bind", vendor: "0x1eff", class: "0x120000", exit: 1, output: "Device " + addr + " is not bound to vfio-pci"},
		{name: "bound-bind", command: "bind", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci", output: "device " + addr + " already bound to vfio-pci"},
		{name: "bound-check", command: "check_bind", vendor: "0x1eff", class: "0x120000", driver: "vfio-pci", output: "All 1 Rebellions NPU devices are bound to VFIO-PCI."},
		{name: "rebind", command: "bind", vendor: "0x1eff", class: "0x120000", driver: "rbln", bind: true, output: "unbinding device " + addr + " from driver rbln"},
		{name: "non-npu", command: "bind", vendor: "0x1eff", class: "0x030000", output: "device " + addr + " is not a npu!"},
		{name: "other-vendor", command: "bind", vendor: "0x1234", class: "0x120000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			pci := filepath.Join(root, "pci")
			devices := filepath.Join(pci, "devices")
			if err := os.MkdirAll(devices, 0o750); err != nil {
				t.Fatal(err)
			}
			write := func(name, contents string, mode os.FileMode) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, []byte(contents), mode); err != nil {
					t.Fatal(err)
				}
			}
			bindFile := filepath.Join(pci, "drivers", "vfio-pci", "bind")
			write(bindFile, "", 0o600)
			dev := filepath.Join(devices, addr)
			if tc.vendor != "" {
				write(filepath.Join(dev, "vendor"), tc.vendor+"\n", 0o600)
				write(filepath.Join(dev, "class"), tc.class+"\n", 0o600)
				write(filepath.Join(dev, "driver_override"), "original\n", 0o600)
			}
			if tc.driver != "" {
				driver := filepath.Join(pci, "drivers", tc.driver)
				write(filepath.Join(driver, "unbind"), "", 0o600)
				if err := os.Symlink(driver, filepath.Join(dev, "driver")); err != nil {
					t.Fatal(err)
				}
			}
			// All writes target this fixture. Module loading is replaced with a
			// successful stub; the test never accesses the host PCI subsystem.
			bin := filepath.Join(root, "bin")
			write(filepath.Join(bin, "chroot"), "#!/bin/sh\nexit 0\n", 0o700)
			name := filepath.Join(root, "vfio-manage.sh")
			write(name, strings.ReplaceAll(script, "/sys/bus/pci", pci), 0o600)
			cmd := exec.CommandContext(t.Context(), bash, name, tc.command, "--all")
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			output, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			if code != tc.exit || !strings.Contains(string(output), tc.output) {
				t.Fatalf("exit=%d, want=%d; output=%s, want substring=%q", code, tc.exit, output, tc.output)
			}
			bound, err := fs.ReadFile(os.DirFS(pci), "drivers/vfio-pci/bind")
			if err != nil {
				t.Fatal(err)
			}
			wantBind, wantOverride := "", "original\n"
			if tc.bind {
				wantBind, wantOverride = addr+"\n", "vfio-pci\n"
			}
			if string(bound) != wantBind {
				t.Fatalf("bind write=%q, want=%q", bound, wantBind)
			}
			if tc.vendor != "" {
				override, err := fs.ReadFile(os.DirFS(pci), "devices/"+addr+"/driver_override")
				if err != nil || string(override) != wantOverride {
					t.Fatalf("override=%q, err=%v; want=%q", override, err, wantOverride)
				}
			}
			if tc.driver == "rbln" && tc.bind {
				unbound, err := fs.ReadFile(os.DirFS(pci), "drivers/rbln/unbind")
				if err != nil || string(unbound) != addr+"\n" {
					t.Fatalf("unbind=%q, err=%v", unbound, err)
				}
			}
		})
	}
}
