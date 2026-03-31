package statusfile

import (
	"fmt"
	"os"
	"path/filepath"
)

func EnsureOutputDir(path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	return nil
}

func Delete(statusFile string) error {
	if err := os.Remove(statusFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove status file %s: %w", statusFile, err)
	}
	return nil
}

func CreateEmpty(dir, filename string) error {
	statusFile := filepath.Join(dir, filename)
	// #nosec G304 -- status file path is derived from validator-managed output paths.
	file, err := os.OpenFile(statusFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create status file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close status file: %w", err)
	}
	return nil
}

func Prepare(outputDir, readyFile string) error {
	if err := Delete(filepath.Join(outputDir, readyFile)); err != nil {
		return err
	}
	return EnsureOutputDir(outputDir)
}

func CreateWithContent(statusFile, content string) error {
	dir := filepath.Dir(statusFile)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(statusFile)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary status file: %w", err)
	}
	if _, err := tmpFile.WriteString(content); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to write temporary status file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary status file: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if err := os.Rename(tmpFile.Name(), statusFile); err != nil {
		return fmt.Errorf("error moving temporary file to '%s': %w", statusFile, err)
	}
	return nil
}
