package driver

import (
	"errors"
	"testing"
)

func TestValidate(t *testing.T) {
	errHost := errors.New("host validation failed")
	errContainer := errors.New("container validation failed")

	type want struct {
		err            bool
		result         Result
		containerCalls int
	}

	cases := map[string]struct {
		reason       string
		hostErr      error
		containerErr error
		want         want
	}{
		"HostDriverSuccess": {
			reason:  "should return host driver result without calling container validation when host validation succeeds",
			hostErr: nil,
			want: want{
				err: false,
				result: Result{
					IsHostDriver:         true,
					DriverRoot:           "/",
					DriverRootCtrPath:    "/host",
					ContainerLibraryPath: driverContainerLibraryPath,
				},
				containerCalls: 0,
			},
		},
		"ContainerDriverSuccess": {
			reason:       "should fall back to container validation when host validation fails",
			hostErr:      errHost,
			containerErr: nil,
			want: want{
				err: false,
				result: Result{
					IsHostDriver:         false,
					DriverRoot:           driverInstallDirDefault,
					DriverRootCtrPath:    "/host",
					ContainerLibraryPath: driverContainerLibraryPath,
				},
				containerCalls: 1,
			},
		},
		"ContainerDriverError": {
			reason:       "should return container validation errors after host validation fails",
			hostErr:      errHost,
			containerErr: errContainer,
			want: want{
				err:            true,
				result:         Result{},
				containerCalls: 1,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			oldValidateHostDriver := validateHostDriverFn
			oldValidateDriverContainer := validateDriverContainerFn
			t.Cleanup(func() {
				validateHostDriverFn = oldValidateHostDriver
				validateDriverContainerFn = oldValidateDriverContainer
			})

			containerCalls := 0
			validateHostDriverFn = func() error {
				return tc.hostErr
			}
			validateDriverContainerFn = func(Config, bool) error {
				containerCalls++
				return tc.containerErr
			}

			got, err := Validate(Config{})

			if tc.want.err && err == nil {
				t.Fatalf("%s: expected error, got nil", tc.reason)
			}
			if !tc.want.err && err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if got != tc.want.result {
				t.Fatalf("%s: got result %+v, want %+v", tc.reason, got, tc.want.result)
			}
			if containerCalls != tc.want.containerCalls {
				t.Fatalf("%s: containerCalls = %d, want %d", tc.reason, containerCalls, tc.want.containerCalls)
			}
		})
	}
}
