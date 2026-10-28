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
