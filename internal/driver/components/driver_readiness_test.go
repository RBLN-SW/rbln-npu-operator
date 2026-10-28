package components

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestDriverEntrypointInvalidatesOnlyItsReadySignals(t *testing.T) {
	h := newTestPatcher(t, "")
	c := h.buildDriverContainer(nil, "driver:test", false)
	root := t.TempDir()
	shared := filepath.Join(root, "validations")
	install := filepath.Join(root, "install")
	started := filepath.Join(root, "started")
	for _, dir := range []string{shared, install} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	marker := filepath.Join(shared, driverCtrReadyFile)
	installMarker := filepath.Join(install, defaultDriverReadyFile)
	state := filepath.Join(root, "rbln-driver.state")
	preserved := []string{state, filepath.Join(shared, "driver-ready"), filepath.Join(shared, "toolkit-ready")}
	for _, file := range append([]string{marker, installMarker}, preserved...) {
		if err := os.WriteFile(file, []byte("previous"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := strings.NewReplacer(driverEntrypointStartedFile, started, consts.ValidationsMountPath, shared, defaultDriverReadyDir, install).Replace(c.Args[0])
	cmd := exec.Command(c.Command[0], c.Command[1], script, "--", "/bin/sh", "-ec",
		`test ! -f "$1"; test ! -f "$2"; test -f "$3"; exit 23`, "--", marker, installMarker, started)
	err := cmd.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 23 {
		t.Fatalf("entrypoint did not see invalidated markers or propagate its exit code: %v", err)
	}
	for _, file := range preserved {
		// #nosec G304 -- each path is a fixture under t.TempDir().
		if content, err := os.ReadFile(file); err != nil || string(content) != "previous" {
			t.Fatalf("must preserve %s: %q, %v", file, content, err)
		}
	}
}

func TestStartupProbeRejectsStaleReadyMarkers(t *testing.T) {
	h := newTestPatcher(t, "")
	if err := h.handleConfigMap(context.Background(), newTestOwner()); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: h.namespace, Name: h.startupProbeConfigMapName()}, cm); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                     string
		started, module, install bool
		wantReady                bool
	}{
		{name: "probe races the new entrypoint", module: true, install: true},
		{name: "install failed", started: true, module: true},
		{name: "module missing", started: true, install: true},
		{name: "current install completed", started: true, module: true, install: true, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			shared := filepath.Join(root, "validations")
			installDir := filepath.Join(root, "install")
			for _, dir := range []string{shared, installDir} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			marker := filepath.Join(shared, driverCtrReadyFile)
			files := map[string]bool{
				marker: true, filepath.Join(root, "started"): tc.started,
				filepath.Join(root, "module"): tc.module, filepath.Join(installDir, defaultDriverReadyFile): tc.install,
			}
			for name, present := range files {
				if present {
					if err := os.WriteFile(name, []byte("old"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			script := strings.NewReplacer(consts.ValidationsMountPath, shared,
				driverEntrypointStartedFile, filepath.Join(root, "started"),
				"/sys/module/rebellions/refcnt", filepath.Join(root, "module")).Replace(cm.Data[startupProbeScriptName])
			cmd := exec.Command("/bin/sh", "-c", script)
			cmd.Env = append(os.Environ(), driverReadyDirEnvName+"="+installDir, driverReadyFileEnvName+"="+defaultDriverReadyFile)
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantReady {
				t.Fatalf("probe = %v, want ready=%v: %s", err, tc.wantReady, output)
			}
			_, statErr := os.Stat(marker)
			if (statErr == nil) != tc.wantReady {
				t.Fatalf("shared marker remains inconsistent with probe: %v", statErr)
			}
		})
	}
}
