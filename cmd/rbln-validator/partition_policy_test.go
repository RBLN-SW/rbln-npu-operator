package main

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	vfiovalidator "github.com/RBLN-SW/rbln-npu-operator/cmd/rbln-validator/vfiopci"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

// Exercise the label contract through both the family decision and the gate.
// The gate only needs the effective PF/VF family; device-config-manager applies
// the VF count. Neither VF count overrides nor fallbacks may change the family.
func TestGatePartitionPolicy(t *testing.T) {
	defaults := []struct {
		name        string
		value       string
		present     bool
		partitioned bool
		invalid     bool
	}{
		{name: "Absent"},
		{name: "PF", value: "pf", present: true},
		{name: "VF1", value: "vf1", present: true, partitioned: true},
		{name: "VF4", value: "vf4", present: true, partitioned: true},
		{name: "LegacyNone", value: "none", present: true, invalid: true},
		{name: "Unsupported", value: "vf2", present: true, invalid: true},
		{name: "Empty", present: true, invalid: true},
		{name: "Uppercase", value: "PF", present: true, invalid: true},
	}
	overrides := []struct {
		name  string
		index string
		value string
	}{
		{name: "Absent"},
		{name: "PF", index: "0", value: "pf"},
		{name: "VF1", index: "0", value: "vf1"},
		{name: "VF4", index: "0", value: "vf4"},
		{name: "LegacyNone", index: "0", value: "none"},
		{name: "Unsupported", index: "0", value: "vf2"},
		{name: "Empty", index: "0"},
		{name: "Uppercase", index: "0", value: "VF4"},
		{name: "InvalidIndex", index: "oops", value: "vf4"},
		{name: "NegativeIndex", index: "-1", value: "vf4"},
		{name: "NonCanonicalIndex", index: "01", value: "vf4"},
	}
	for _, workload := range []string{consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough} {
		for _, nodeDefault := range defaults {
			for _, override := range overrides {
				t.Run(workload+"/"+nodeDefault.name+"/"+override.name, func(t *testing.T) {
					labels := map[string]string{consts.RBLNWorkloadConfigLabelKey: workload}
					if nodeDefault.present {
						labels[consts.RBLNPartitionLabelKey] = nodeDefault.value
					}
					if override.name != "Absent" {
						labels[consts.RBLNPartitionIndexLabelPrefix+override.index] = override.value
					}
					original := maps.Clone(labels)
					partitioned, err := nodeIsPartitioned(labels)
					if (err != nil) != nodeDefault.invalid || partitioned != nodeDefault.partitioned {
						t.Fatalf("nodeIsPartitioned(%v) = %v, %v; want partitioned=%v invalid=%v",
							labels, partitioned, err, nodeDefault.partitioned, nodeDefault.invalid)
					}

					outputDir := t.TempDir()
					writeMarker := func(name string) {
						t.Helper()
						if err := os.WriteFile(filepath.Join(outputDir, name), nil, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					writeMarker(toolkitReadyFile)
					vm := workload == consts.RBLNWorkloadConfigVMPassthrough
					if vm {
						// VM consumers always wait for the manager to finish binding,
						// even when the effective node family is PF.
						writeMarker(consts.PartitionReadyFileName)
					}
					wantSleeps := 0
					if nodeDefault.invalid || (!vm && nodeDefault.partitioned) {
						wantSleeps = 1
					}
					sleeps, driverChecks, vfioChecks := 0, 0, 0
					rt := gateRuntime{
						getNodeLabels: func(context.Context, string) (map[string]string, error) {
							if nodeDefault.invalid && sleeps > 0 {
								fixed := maps.Clone(labels)
								fixed[consts.RBLNPartitionLabelKey] = "pf"
								return fixed, nil
							}
							return labels, nil
						},
						checkDriver: func(context.Context) error {
							driverChecks++
							return nil
						},
						validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
							vfioChecks++
							return vfiovalidator.Result{}, nil
						},
						sleep: func(time.Duration) {
							sleeps++
							if sleeps > wantSleeps {
								t.Fatal("gate blocked despite ready effective partition")
							}
							if nodeDefault.invalid {
								if driverChecks != 0 || vfioChecks != 0 {
									t.Fatal("invalid node default reached readiness checks")
								}
							} else {
								writeMarker(consts.PartitionReadyFileName)
							}
						},
					}
					cfg := &gateConfig{
						nodeName: "node-1", outputDir: outputDir,
						component: consts.RBLNDRAKubeletPluginName,
					}
					if err := runGate(context.Background(), cfg, rt); err != nil {
						t.Fatal(err)
					}
					if sleeps != wantSleeps {
						t.Fatalf("sleeps=%d, want %d", sleeps, wantSleeps)
					}
					if vm && (vfioChecks != 1 || driverChecks != 0) {
						t.Fatalf("VM gate: vfioChecks=%d driverChecks=%d", vfioChecks, driverChecks)
					}
					if !vm && (vfioChecks != 0 || driverChecks == 0) {
						t.Fatalf("container gate: vfioChecks=%d driverChecks=%d", vfioChecks, driverChecks)
					}
					if !maps.Equal(labels, original) {
						t.Fatalf("gate changed labels: got %v, want %v", labels, original)
					}
				})
			}
		}
	}
}
