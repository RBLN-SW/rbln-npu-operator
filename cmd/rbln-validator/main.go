package main

import (
	"log/slog"
	"os"

	"github.com/rebellions-sw/rbln-npu-operator/internal/logging"
)

func run() error {
	return NewRBLNValidatorApp().Execute()
}

func main() {
	logging.SetupFromEnv()
	if err := run(); err != nil {
		slog.Error("Command execution failed", "err", err)
		os.Exit(1)
	}
}
