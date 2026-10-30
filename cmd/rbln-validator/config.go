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
	"os"
	"strconv"

	"github.com/spf13/cobra"

	drivervalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/driver"
	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	defaultOutputDir            = "/run/rbln/validations"
	defaultSleepIntervalSeconds = 5

	envOutputDir            = "OUTPUT_DIR"
	envSleepIntervalSeconds = "SLEEP_INTERVAL_SECONDS"
	envNodeName             = "NODE_NAME"
	envOperatorNamespace    = "OPERATOR_NAMESPACE"
	envDefaultWorkloadType  = "DEFAULT_WORKLOAD_TYPE"
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
	defaultWorkload      string
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
		NodeName:             os.Getenv(envNodeName),
		Namespace:            os.Getenv(envOperatorNamespace),
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
		defaultWorkload:      envString(envDefaultWorkloadType, consts.RBLNWorkloadConfigContainer),
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
