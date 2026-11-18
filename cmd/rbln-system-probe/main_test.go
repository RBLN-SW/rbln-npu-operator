package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestProbeRequiresLoadedModule(t *testing.T) {
	root := t.TempDir()
	if err := probe(t.Context(), root); err == nil {
		t.Fatal("accepted absent module")
	}
	module := filepath.Join(root, "module", "rebellions")
	if err := os.MkdirAll(module, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := probe(t.Context(), root); err == nil {
		t.Fatal("accepted incomplete module")
	}
	if err := os.WriteFile(filepath.Join(module, "refcnt"), []byte("0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := probe(t.Context(), root); err != nil {
		t.Fatal(err)
	}
}

func TestInstallCopiesExecutable(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "probe")
	if err := install(destination); err != nil {
		t.Fatal(err)
	}
	original, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G304 -- original is this test executable from os.Executable.
	a, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G304 -- destination is a test-owned path under t.TempDir.
	b, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("installed probe differs from its executable")
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("mode=%v err=%v", info, err)
	}
}
