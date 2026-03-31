package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	drivervalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/driver"
)

func TestValidateDriver(t *testing.T) {
	errNotReady := errors.New("not ready")
	errWriteStatus := errors.New("write status failed")

	successfulResult := drivervalidator.Result{
		IsHostDriver:         true,
		DriverRoot:           "/",
		DriverRootCtrPath:    "/host",
		ContainerLibraryPath: "/usr/local/lib/rbln",
	}

	type args struct {
		cfg            drivervalidator.Config
		precreateReady bool
	}

	type want struct {
		err              bool
		driverReady      bool
		writeStatusCalls int
		readyFileContent string
	}

	cases := map[string]struct {
		reason string
		args   args
		setup  func() driverRuntime
		want   want
	}{
		"SuccessCreatesDriverReady": {
			reason: "should validate successfully and write driver-ready content",
			args: args{
				precreateReady: true,
			},
			setup: func() driverRuntime {
				return driverRuntime{
					validate: func(drivervalidator.Config) (drivervalidator.Result, error) {
						return successfulResult, nil
					},
					writeStatus: drivervalidator.WriteStatusFile,
				}
			},
			want: want{
				err:              false,
				driverReady:      true,
				writeStatusCalls: 1,
				readyFileContent: strings.Join([]string{
					"IS_HOST_DRIVER=true",
					"RBLN_CTK_DAEMON_HOST_ROOT=/host",
					"RBLN_CTK_DAEMON_DRIVER_ROOT=/",
					"RBLN_CTK_DAEMON_CONTAINER_LIBRARY_PATH=/usr/local/lib/rbln",
					"",
				}, "\n"),
			},
		},
		"ValidationErrorRemovesStaleReadyFile": {
			reason: "should return validation errors and not leave a stale driver-ready file behind",
			args: args{
				precreateReady: true,
			},
			setup: func() driverRuntime {
				return driverRuntime{
					validate: func(drivervalidator.Config) (drivervalidator.Result, error) {
						return drivervalidator.Result{}, errNotReady
					},
					writeStatus: func(string, drivervalidator.Result) error {
						return nil
					},
				}
			},
			want: want{
				err:              true,
				driverReady:      false,
				writeStatusCalls: 0,
			},
		},
		"WriteStatusErrorIsReturned": {
			reason: "should return write status errors after successful validation",
			args: args{
				precreateReady: true,
			},
			setup: func() driverRuntime {
				return driverRuntime{
					validate: func(drivervalidator.Config) (drivervalidator.Result, error) {
						return successfulResult, nil
					},
					writeStatus: func(string, drivervalidator.Result) error {
						return errWriteStatus
					},
				}
			},
			want: want{
				err:              true,
				driverReady:      false,
				writeStatusCalls: 1,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			outputDir := t.TempDir()
			cfg := tc.args.cfg
			cfg.OutputDir = outputDir

			readyFile := filepath.Join(outputDir, drivervalidator.ReadyFileName)
			if tc.args.precreateReady {
				if err := os.WriteFile(readyFile, []byte("stale"), 0o600); err != nil {
					t.Fatalf("%s: precreate ready file: %v", tc.reason, err)
				}
			}

			writeStatusCalls := 0
			rt := tc.setup()
			originalWriteStatus := rt.writeStatus
			rt.writeStatus = func(outputDir string, result drivervalidator.Result) error {
				writeStatusCalls++
				return originalWriteStatus(outputDir, result)
			}

			err := validateDriver(cfg, rt)

			if tc.want.err && err == nil {
				t.Fatalf("%s: expected error, got nil", tc.reason)
			}
			if !tc.want.err && err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}

			_, statErr := os.Stat(readyFile)
			gotReady := statErr == nil
			if gotReady != tc.want.driverReady {
				t.Fatalf("%s: driver-ready = %v, want %v", tc.reason, gotReady, tc.want.driverReady)
			}

			if writeStatusCalls != tc.want.writeStatusCalls {
				t.Fatalf("%s: writeStatusCalls = %d, want %d", tc.reason, writeStatusCalls, tc.want.writeStatusCalls)
			}

			if tc.want.readyFileContent != "" {
				// #nosec G304 -- readyFile is created under t.TempDir() in this test.
				content, err := os.ReadFile(readyFile)
				if err != nil {
					t.Fatalf("%s: read ready file: %v", tc.reason, err)
				}
				if string(content) != tc.want.readyFileContent {
					t.Fatalf("%s: unexpected ready file content:\n%s", tc.reason, string(content))
				}
			}

			if tc.args.precreateReady && !tc.want.driverReady {
				if _, err := os.Stat(readyFile); !os.IsNotExist(err) {
					t.Fatalf("%s: stale ready file should be removed", tc.reason)
				}
			}
		})
	}
}
