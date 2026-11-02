package main

import (
	"log/slog"
	"os"

	"github.com/RBLN-SW/rbln-npu-operator/internal/logging"
)

func run() error {
	return NewRBLNValidatorApp().Execute()
}

func main() {
	logging.SetupFromEnv()
	if err := run(); err != nil {
		slog.Error("Command execution failed", "error", err)
		os.Exit(1)
	}
}
