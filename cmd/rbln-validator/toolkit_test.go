/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeToolkitValidator struct {
	errs []error
	call int
}

func (f *fakeToolkitValidator) Validate(context.Context, string) error {
	if f.call >= len(f.errs) {
		return nil
	}
	err := f.errs[f.call]
	f.call++
	return err
}

func TestValidateToolkit(t *testing.T) {
	errNotReady := errors.New("not ready")

	type fields struct {
		validator *fakeToolkitValidator
	}

	type args struct {
		cfg *toolkitConfig
	}

	type want struct {
		err            bool
		toolkitReady   bool
		sleepCallCount int
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"SuccessImmediately": {
			reason: "should create toolkit-ready when validation succeeds immediately",
			fields: fields{
				validator: &fakeToolkitValidator{},
			},
			args: args{
				cfg: &toolkitConfig{
					sleepIntervalSeconds: 1,
				},
			},
			want: want{
				err:          false,
				toolkitReady: true,
			},
		},
		"RetryUntilSuccess": {
			reason: "should retry once and then create toolkit-ready",
			fields: fields{
				validator: &fakeToolkitValidator{
					errs: []error{errNotReady, nil},
				},
			},
			args: args{
				cfg: &toolkitConfig{
					sleepIntervalSeconds: 1,
				},
			},
			want: want{
				err:            false,
				toolkitReady:   true,
				sleepCallCount: 1,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			outputDir := t.TempDir()
			tc.args.cfg.outputDir = outputDir

			sleepCalls := 0
			rt := toolkitRuntime{
				validator: tc.fields.validator,
				cdiRoot:   "/unused",
				sleep: func(time.Duration) {
					sleepCalls++
				},
			}

			err := validateToolkit(context.Background(), tc.args.cfg, rt)

			if tc.want.err && err == nil {
				t.Fatalf("%s: expected error, got nil", tc.reason)
			}
			if !tc.want.err && err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}

			_, statErr := os.Stat(filepath.Join(outputDir, toolkitReadyFile))
			gotReady := statErr == nil
			if gotReady != tc.want.toolkitReady {
				t.Fatalf("%s: toolkit-ready = %v, want %v", tc.reason, gotReady, tc.want.toolkitReady)
			}

			if sleepCalls != tc.want.sleepCallCount {
				t.Fatalf("%s: sleepCalls = %d, want %d", tc.reason, sleepCalls, tc.want.sleepCallCount)
			}
		})
	}
}
