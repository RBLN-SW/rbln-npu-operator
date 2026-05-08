package main

import (
	"errors"
	"os"
	"path/filepath"
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
