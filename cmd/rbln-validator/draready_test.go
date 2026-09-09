package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

func TestWaitDRAReady(t *testing.T) {
	errNotBound := errors.New("npu devices not bound to vfio-pci")

	cases := map[string]struct {
		reason         string
		toolkitReadyAt int // iteration index from which toolkit-ready exists; -1 = never
		vfioReadyAt    int // iteration index from which every NPU is on vfio-pci; -1 = never
		cancelOnSleep  int // sleep call (1-based) that cancels the context; 0 = never
		wantErr        bool
		wantSleeps     int
		wantVFIOCalls  int
	}{
		"ToolkitReadyImmediately": {
			reason:         "container node with the stack already up passes without touching sysfs",
			toolkitReadyAt: 0,
			vfioReadyAt:    -1,
			wantSleeps:     0,
			wantVFIOCalls:  0,
		},
		"VFIOBoundImmediately": {
			reason:         "vm-passthrough node whose NPUs are already on vfio-pci passes on the first probe",
			toolkitReadyAt: -1,
			vfioReadyAt:    0,
			wantSleeps:     0,
			wantVFIOCalls:  1,
		},
		"ToolkitAppearsAfterRetries": {
			reason:         "container node: the gate waits out driver install and toolkit CDI generation",
			toolkitReadyAt: 2,
			vfioReadyAt:    -1,
			wantSleeps:     2,
			wantVFIOCalls:  2,
		},
		"VFIOBindsAfterRetries": {
			reason:         "vm-passthrough node: the gate waits out vfio-manager binding",
			toolkitReadyAt: -1,
			vfioReadyAt:    2,
			wantSleeps:     2,
			wantVFIOCalls:  3,
		},
		"ContextCancelled": {
			reason:         "a cancelled context ends the wait with an error instead of looping forever",
			toolkitReadyAt: -1,
			vfioReadyAt:    -1,
			cancelOnSleep:  1,
			wantErr:        true,
			wantSleeps:     1,
			wantVFIOCalls:  1,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			iteration, sleeps, vfioCalls := 0, 0, 0
			rt := draReadyRuntime{
				toolkitReady: func(string) (bool, error) {
					return tc.toolkitReadyAt >= 0 && iteration >= tc.toolkitReadyAt, nil
				},
				validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					vfioCalls++
					if tc.vfioReadyAt >= 0 && iteration >= tc.vfioReadyAt {
						return vfiovalidator.Result{BoundDevices: []string{"0000:01:00.0"}}, nil
					}
					return vfiovalidator.Result{}, errNotBound
				},
				sleep: func(context.Context, time.Duration) error {
					sleeps++
					iteration++
					if tc.cancelOnSleep > 0 && sleeps >= tc.cancelOnSleep {
						return context.Canceled
					}
					return nil
				},
			}

			err := waitDRAReady(context.Background(), t.TempDir(), vfiovalidator.Config{}, time.Second, rt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%s: err = %v, wantErr %t", tc.reason, err, tc.wantErr)
			}
			if sleeps != tc.wantSleeps {
				t.Errorf("%s: sleeps = %d, want %d", tc.reason, sleeps, tc.wantSleeps)
			}
			if vfioCalls != tc.wantVFIOCalls {
				t.Errorf("%s: vfio validate calls = %d, want %d", tc.reason, vfioCalls, tc.wantVFIOCalls)
			}
		})
	}
}

// The gate must not write any *-ready marker: vfio-pci-ready belongs to the
// sandbox-device-plugin's init container and toolkit-ready to the validator.
func TestWaitDRAReadyWritesNoStatusFile(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outputDir, toolkitReadyFile), nil, 0o600); err != nil {
		t.Fatalf("precreate toolkit-ready: %v", err)
	}

	rt := defaultDRAReadyRuntime()
	rt.validateVFIO = func(vfiovalidator.Config) (vfiovalidator.Result, error) {
		t.Fatal("validateVFIO must not run when toolkit-ready exists")
		return vfiovalidator.Result{}, nil
	}

	if err := waitDRAReady(context.Background(), outputDir, vfiovalidator.Config{}, time.Second, rt); err != nil {
		t.Fatalf("waitDRAReady: %v", err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != toolkitReadyFile {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("output dir = %v, want only %s", names, toolkitReadyFile)
	}
}

func TestToolkitReadyMarkerExists(t *testing.T) {
	outputDir := t.TempDir()

	ready, err := toolkitReadyMarkerExists(outputDir)
	if err != nil || ready {
		t.Fatalf("missing marker: ready=%t err=%v, want false/nil", ready, err)
	}

	if err := os.WriteFile(filepath.Join(outputDir, toolkitReadyFile), nil, 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	ready, err = toolkitReadyMarkerExists(outputDir)
	if err != nil || !ready {
		t.Fatalf("present marker: ready=%t err=%v, want true/nil", ready, err)
	}
}
