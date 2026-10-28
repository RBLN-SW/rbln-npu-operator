package driver

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitForDriverInstallRechecksProducer(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, driverContainerReadyFile)
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	checks, validations, sleeps := 0, 0, 0
	check := func() error {
		checks++
		// First attempt sees a crashed container despite the marker. The
		// second attempt loses readiness during library validation.
		if checks == 1 || checks == 3 {
			return errors.New("container not Ready")
		}
		return nil
	}
	err := waitForDriverInstall(Config{OutputDir: dir}, check, func() error {
		validations++
		return nil
	}, func(time.Duration) { sleeps++ })
	if err != nil || checks != 5 || validations != 2 || sleeps != 2 {
		t.Fatalf("producer state was not rechecked: checks=%d validations=%d sleeps=%d err=%v",
			checks, validations, sleeps, err)
	}
}

func TestWaitForFile(t *testing.T) {
	t.Run("returns immediately when the marker exists", func(t *testing.T) {
		dir := t.TempDir()
		marker := filepath.Join(dir, driverContainerReadyFile)
		if err := os.WriteFile(marker, nil, 0o600); err != nil {
			t.Fatalf("write marker: %v", err)
		}
		sleeps := 0
		if err := waitForFile(marker, time.Second, func(time.Duration) { sleeps++ }, false); err != nil {
			t.Fatalf("waitForFile() error = %v", err)
		}
		if sleeps != 0 {
			t.Fatalf("sleeps = %d, want 0", sleeps)
		}
	})

	t.Run("polls until the marker appears", func(t *testing.T) {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
		t.Cleanup(func() { slog.SetDefault(prev) })

		dir := t.TempDir()
		marker := filepath.Join(dir, driverContainerReadyFile)
		sleeps := 0
		sleep := func(time.Duration) {
			sleeps++
			if sleeps == 2 {
				if err := os.WriteFile(marker, nil, 0o600); err != nil {
					t.Fatalf("write marker: %v", err)
				}
			}
		}
		if err := waitForFile(marker, time.Second, sleep, false); err != nil {
			t.Fatalf("waitForFile() error = %v", err)
		}
		if sleeps != 2 {
			t.Fatalf("sleeps = %d, want 2", sleeps)
		}

		output := buf.String()
		wantNotFoundMsg := `"msg":"Driver container ready marker not found, retrying"`
		if got := strings.Count(output, wantNotFoundMsg); got != 2 {
			t.Fatalf("retry log lines = %d, want 2; output: %s", got, output)
		}
		wantFoundMsg := `"msg":"Driver container ready marker found"`
		if got := strings.Count(output, wantFoundMsg); got != 1 {
			t.Fatalf("found log lines = %d, want 1; output: %s", got, output)
		}
		if !strings.Contains(output, `"path":"`+marker+`"`) {
			t.Fatalf("log output missing marker path %q; output: %s", marker, output)
		}
	})

	t.Run("silent suppresses both log lines", func(t *testing.T) {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
		t.Cleanup(func() { slog.SetDefault(prev) })

		dir := t.TempDir()
		marker := filepath.Join(dir, driverContainerReadyFile)
		sleep := func(time.Duration) {
			if err := os.WriteFile(marker, nil, 0o600); err != nil {
				t.Fatalf("write marker: %v", err)
			}
		}
		if err := waitForFile(marker, time.Second, sleep, true); err != nil {
			t.Fatalf("waitForFile() error = %v", err)
		}
		if buf.Len() != 0 {
			t.Fatalf("log output = %q, want empty", buf.String())
		}
	})

	t.Run("propagates stat errors other than not-exist", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "regular-file")
		if err := os.WriteFile(file, nil, 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		// A regular file used as a path component makes stat fail with ENOTDIR,
		// which is not os.ErrNotExist.
		path := filepath.Join(file, driverContainerReadyFile)
		err := waitForFile(path, time.Second, func(time.Duration) { t.Fatal("must not sleep") }, false)
		if err == nil {
			t.Fatal("waitForFile() error = nil, want ENOTDIR")
		}
	})
}
