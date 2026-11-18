// rbln-system-probe is copied into the driver Pod to keep system layout
// compatibility in the SDK, independent of the SSW container image.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	host "github.com/rbln-sw/rblnlib-go/pkg/host/v1"
	system "github.com/rbln-sw/rblnlib-go/pkg/system/v1"
)

const probeInstallPath = "/opt/rbln-system-probe/rbln-system-probe"

func probe(ctx context.Context, sysRoot string) error {
	c, err := host.New(host.Options{SysRoot: sysRoot})
	if err != nil {
		return err
	}
	m, err := c.Module(ctx, system.NativeDriver)
	if err != nil {
		return err
	}
	if !m.Loaded || m.RefCount == nil {
		return fmt.Errorf("rebellions kernel module not loaded")
	}
	return nil
}

func install(path string) (err error) {
	source, err := os.Executable()
	if err != nil {
		return err
	}
	// #nosec G304 -- source is this running executable, resolved by os.Executable.
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	// #nosec G304 -- production destination is the fixed probeInstallPath; tests use a temporary directory.
	output, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Chmod(0o755), output.Close())
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "install" {
		if err := install(probeInstallPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if err := probe(context.Background(), "/sys"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
