package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

func TestValidateVFIOPCI(t *testing.T) {
	errNotReady := errors.New("not ready")
	errWriteStatus := errors.New("write status failed")

	successResult := vfiovalidator.Result{BoundDevices: []string{"0000:01:00.0"}}

	cases := map[string]struct {
		reason          string
		validateReturns []validateReturn
		writeStatusErr  error
		wantErr         bool
		wantReady       bool
		wantSleeps      int
	}{
		"SuccessImmediately": {
			reason:          "single successful Validate writes ready file without sleeping",
			validateReturns: []validateReturn{{result: successResult}},
			wantReady:       true,
		},
		"RetryUntilSuccess": {
			reason: "transient errors trigger sleeps before eventual success",
			validateReturns: []validateReturn{
				{err: errNotReady},
				{err: errNotReady},
				{result: successResult},
			},
			wantReady:  true,
			wantSleeps: 2,
		},
		"WriteStatusError": {
			reason:          "WriteStatusFile errors propagate to caller",
			validateReturns: []validateReturn{{result: successResult}},
			writeStatusErr:  errWriteStatus,
			wantErr:         true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			outputDir := t.TempDir()
			readyPath := filepath.Join(outputDir, vfiovalidator.ReadyFileName)
			if err := os.WriteFile(readyPath, []byte("stale"), 0o600); err != nil {
				t.Fatalf("precreate stale ready file: %v", err)
			}

			calls := 0
			sleeps := 0
			rt := vfioPCIRuntime{
				validate: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					if calls >= len(tc.validateReturns) {
						t.Fatalf("validate called %d times, only %d returns prepared", calls+1, len(tc.validateReturns))
					}
					ret := tc.validateReturns[calls]
					calls++
					return ret.result, ret.err
				},
				writeStatus: func(dir string, _ vfiovalidator.Result) error {
					if tc.writeStatusErr != nil {
						return tc.writeStatusErr
					}
					return vfiovalidator.WriteStatusFile(dir, successResult)
				},
				sleep: func(time.Duration) { sleeps++ },
			}

			err := validateVFIOPCI(vfiovalidator.Config{OutputDir: outputDir}, rt)

			if tc.wantErr && err == nil {
				t.Fatalf("%s: expected error, got nil", tc.reason)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}

			_, statErr := os.Stat(readyPath)
			gotReady := statErr == nil
			if gotReady != tc.wantReady {
				t.Fatalf("%s: ready file present = %v, want %v", tc.reason, gotReady, tc.wantReady)
			}
			if sleeps != tc.wantSleeps {
				t.Fatalf("%s: sleeps = %d, want %d", tc.reason, sleeps, tc.wantSleeps)
			}
		})
	}
}

type validateReturn struct {
	result vfiovalidator.Result
	err    error
}

func TestAssertRBLNBoundPolls(t *testing.T) {
	errScan := errors.New("scan failed")
	clean := vfiovalidator.AssertResult{CleanDevices: []string{"0000:05:00.0"}}
	dirtyUnbound := vfiovalidator.AssertResult{
		DirtyDevices: []vfiovalidator.DirtyDevice{{BDF: "0000:05:00.0", CurrentDriver: ""}},
	}
	dirtyVFIO := vfiovalidator.AssertResult{
		DirtyDevices: []vfiovalidator.DirtyDevice{{BDF: "0000:06:00.0", CurrentDriver: "vfio-pci"}},
	}

	cases := map[string]struct {
		reason        string
		assertReturns []assertReturn
		warnAfter     time.Duration
		wantErr       error
		wantSleeps    int
		wantContains  []string
		wantInfoLines int
		wantWarnLines int
	}{
		"CleanImmediately": {
			reason:        "all devices on the rbln driver returns without sleeping",
			assertReturns: []assertReturn{{result: clean}},
			warnAfter:     time.Hour,
		},
		"RetryUntilClean": {
			reason: "devices unbound while k8s-driver-manager reloads the module are transient: retry, never exit non-zero",
			assertReturns: []assertReturn{
				{result: dirtyUnbound},
				{result: dirtyVFIO},
				{result: clean},
			},
			warnAfter:    time.Hour,
			wantSleeps:   2,
			wantContains: []string{"0000:05:00.0=unbound", "0000:06:00.0=vfio-pci"},
		},
		"ScanError": {
			reason:        "an unreadable sysfs is a real failure and propagates",
			assertReturns: []assertReturn{{err: errScan}},
			warnAfter:     time.Hour,
			wantErr:       errScan,
		},
		"EscalatesToWarnAfterWindow": {
			reason: "past the normal driver-install window a stuck binding must be alertable, not silent info",
			assertReturns: []assertReturn{
				{result: dirtyUnbound},
				{result: dirtyUnbound},
				{result: dirtyUnbound},
				{result: clean},
			},
			warnAfter:     2 * time.Second,
			wantSleeps:    3,
			wantInfoLines: 2,
			wantWarnLines: 1,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			calls, sleeps := 0, 0
			rt := assertRuntime{
				assert: func(vfiovalidator.AssertConfig) (vfiovalidator.AssertResult, error) {
					if calls >= len(tc.assertReturns) {
						t.Fatalf("assert called %d times, only %d returns prepared", calls+1, len(tc.assertReturns))
					}
					ret := tc.assertReturns[calls]
					calls++
					return ret.result, ret.err
				},
				sleep:     func(time.Duration) { sleeps++ },
				interval:  time.Second,
				warnAfter: tc.warnAfter,
			}

			err := assertRBLNBound(vfiovalidator.AssertConfig{}, rt)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("%s: assertRBLNBound() error = %v, want %v", tc.reason, err, tc.wantErr)
			}
			if sleeps != tc.wantSleeps {
				t.Fatalf("%s: sleeps = %d, want %d", tc.reason, sleeps, tc.wantSleeps)
			}

			output := buf.String()
			for _, want := range tc.wantContains {
				if !strings.Contains(output, want) {
					t.Fatalf("%s: log output missing %q; got %s", tc.reason, want, output)
				}
			}

			if tc.wantInfoLines == 0 && tc.wantWarnLines == 0 {
				return
			}
			infoLines, warnLines := 0, 0
			sawWarn := false
			for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
				if line == "" {
					continue
				}
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatalf("%s: unmarshal log line: %v", tc.reason, err)
				}
				switch entry["msg"] {
				case "RBLN binding not ready, retrying":
					if entry["level"] != "INFO" {
						t.Fatalf("%s: retry msg at level %v, want INFO", tc.reason, entry["level"])
					}
					if sawWarn {
						t.Fatalf("%s: info retry line after a warn line; want all info before the escalation", tc.reason)
					}
					infoLines++
				case "RBLN binding still not ready, retrying":
					if entry["level"] != "WARN" {
						t.Fatalf("%s: escalated retry msg at level %v, want WARN", tc.reason, entry["level"])
					}
					sawWarn = true
					warnLines++
				}
			}
			if infoLines != tc.wantInfoLines {
				t.Fatalf("%s: info lines = %d, want %d", tc.reason, infoLines, tc.wantInfoLines)
			}
			if warnLines != tc.wantWarnLines {
				t.Fatalf("%s: warn lines = %d, want %d", tc.reason, warnLines, tc.wantWarnLines)
			}
		})
	}
}

type assertReturn struct {
	result vfiovalidator.AssertResult
	err    error
}
