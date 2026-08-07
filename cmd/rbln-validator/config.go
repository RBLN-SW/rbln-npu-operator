package main

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"

	drivervalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/driver"
	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
)

const (
	defaultOutputDir            = "/run/rbln/validations"
	defaultSleepIntervalSeconds = 5

	envOutputDir            = "OUTPUT_DIR"
	envSleepIntervalSeconds = "SLEEP_INTERVAL_SECONDS"
	envNodeName             = "NODE_NAME"
)

type rootConfig struct {
	outputDir            string
	sleepIntervalSeconds int
}

type toolkitConfig struct {
	outputDir            string
	sleepIntervalSeconds int
}

type gateConfig struct {
	outputDir            string
	sleepIntervalSeconds int
	component            string
	nodeName             string
}

func newRootConfig() *rootConfig {
	return &rootConfig{
		outputDir:            envString(envOutputDir, defaultOutputDir),
		sleepIntervalSeconds: envInt(envSleepIntervalSeconds, defaultSleepIntervalSeconds),
	}
}

func (b *rootConfig) bindFlags(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.StringVar(&b.outputDir, "output-dir", b.outputDir, "output directory for validation status files")
	flags.IntVar(
		&b.sleepIntervalSeconds,
		"sleep-interval-seconds",
		b.sleepIntervalSeconds,
		"sleep interval in seconds between retries",
	)
}

func (o *rootConfig) driverConfig() drivervalidator.Config {
	return drivervalidator.Config{
		OutputDir:            o.outputDir,
		SleepIntervalSeconds: o.sleepIntervalSeconds,
	}
}

func (o *rootConfig) toolkitConfig() *toolkitConfig {
	return &toolkitConfig{
		outputDir:            o.outputDir,
		sleepIntervalSeconds: o.sleepIntervalSeconds,
	}
}

func (o *rootConfig) gateConfig(component string) *gateConfig {
	return &gateConfig{
		outputDir:            o.outputDir,
		sleepIntervalSeconds: o.sleepIntervalSeconds,
		component:            component,
		nodeName:             os.Getenv(envNodeName),
	}
}

func (o *rootConfig) vfioPCIConfig() vfiovalidator.Config {
	return vfiovalidator.Config{
		OutputDir:            o.outputDir,
		SleepIntervalSeconds: o.sleepIntervalSeconds,
	}
}

func envString(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func envInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}
