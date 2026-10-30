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

package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostDriverValidatedFromPublishedResult(t *testing.T) {
	dir := t.TempDir()
	if got, err := HostDriverValidated(dir); err != nil || got {
		t.Fatalf("missing validation record = %v, %v", got, err)
	}
	for _, result := range []Result{
		{IsHostDriver: true, DriverRoot: "/", DriverRootCtrPath: "/host"},
		{IsHostDriver: false, DriverRoot: "/run/rbln/driver", DriverRootCtrPath: "/host"},
	} {
		if err := WriteStatusFile(dir, result); err != nil {
			t.Fatal(err)
		}
		if got, err := HostDriverValidated(dir); err != nil || got != result.IsHostDriver {
			t.Fatalf("published host=%v read as %v, %v", result.IsHostDriver, got, err)
		}
	}
}

func TestHostDriverValidatedRejectsInvalidRecords(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "empty record"},
		{name: "missing host flag", content: "RBLN_CTK_DAEMON_DRIVER_ROOT=/\n"},
		{name: "missing root", content: "IS_HOST_DRIVER=true\n"},
		{name: "container root", content: "IS_HOST_DRIVER=true\nRBLN_CTK_DAEMON_DRIVER_ROOT=/run/rbln/driver\n"},
		{
			name: "ambiguous host flag", wantErr: true,
			content: "IS_HOST_DRIVER=false\nIS_HOST_DRIVER=true\nRBLN_CTK_DAEMON_DRIVER_ROOT=/\n",
		},
		{
			name: "ambiguous root", wantErr: true,
			content: "IS_HOST_DRIVER=true\nRBLN_CTK_DAEMON_DRIVER_ROOT=/\nRBLN_CTK_DAEMON_DRIVER_ROOT=/old\n",
		},
		{name: "malformed record", content: "not a validation record", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, driverReadyFile), []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if got, err := HostDriverValidated(dir); got || (err != nil) != tc.wantErr {
				t.Fatalf("record accepted or error hidden: validated=%v err=%v", got, err)
			}
		})
	}
	t.Run("read error", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, driverReadyFile), 0o700); err != nil {
			t.Fatal(err)
		}
		if got, err := HostDriverValidated(dir); got || err == nil {
			t.Fatalf("unreadable record accepted: validated=%v err=%v", got, err)
		}
	})
}
