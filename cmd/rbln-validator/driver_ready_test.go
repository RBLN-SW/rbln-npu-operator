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

func TestWaitForCurrentDriverRejectsStaleMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".driver-ctr-ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := waitForCurrentDriver(ctx, dir, time.Millisecond, func(context.Context) error {
		cancel()
		return errors.New("current container crashed")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("stale marker passed readiness: %v", err)
	}
	checks := 0
	err = waitForCurrentDriver(context.Background(), dir, time.Millisecond, func(context.Context) error {
		checks++
		if checks == 1 {
			return errors.New("new container still installing")
		}
		return nil
	})
	if err != nil || checks != 2 {
		t.Fatalf("must wait for current container: checks=%d err=%v", checks, err)
	}
}
