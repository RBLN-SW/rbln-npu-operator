package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	vfiovalidator "github.com/rebellions-sw/rbln-npu-operator/cmd/rbln-validator/vfiopci"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type nodeLabelsResult struct {
	labels map[string]string
	err    error
}

type fakeNodeLabelsGetter struct {
	results []nodeLabelsResult
	call    int
}

func (f *fakeNodeLabelsGetter) get(context.Context, string) (map[string]string, error) {
	result := f.results[f.call]
	if f.call < len(f.results)-1 {
		f.call++
	}
	return result.labels, result.err
}

func TestRunGate(t *testing.T) {
	errAPI := errors.New("api unavailable")

	type fields struct {
		getter *fakeNodeLabelsGetter
	}

	type args struct {
		component      string
		nodeName       string
		existingFiles  []string
		createOnSleeps map[int][]string
	}

	type want struct {
		err            bool
		sleepCallCount int
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"DevicePluginUnpartitionedWaitsToolkitOnly": {
			reason: "on a node without partition labels the device plugin only needs toolkit-ready",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{}},
				}},
			},
			args: args{
				component:     consts.RBLNDevicePluginName,
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{sleepCallCount: 0},
		},
		"DevicePluginPartitionedWaitsPartitionReady": {
			reason: "on a partitioned node the device plugin must additionally block until partition-ready appears",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
				}},
			},
			args: args{
				component:      consts.RBLNDevicePluginName,
				nodeName:       "node-1",
				existingFiles:  []string{toolkitReadyFile},
				createOnSleeps: map[int][]string{1: {consts.PartitionReadyFileName}},
			},
			want: want{sleepCallCount: 1},
		},
		"IndexOverrideAloneMakesNodePartitioned": {
			reason: "a single per-index vf override makes the node partitioned even when the base label is none",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{
						consts.RBLNPartitionLabelKey:               consts.RBLNPartitionModeNone,
						consts.RBLNPartitionIndexLabelPrefix + "6": consts.RBLNPartitionModeVF1,
					}},
				}},
			},
			args: args{
				component:      consts.RBLNDevicePluginName,
				nodeName:       "node-1",
				existingFiles:  []string{toolkitReadyFile},
				createOnSleeps: map[int][]string{1: {consts.PartitionReadyFileName}},
			},
			want: want{sleepCallCount: 1},
		},
		"MetricsExporterIgnoresPartitioning": {
			reason: "metrics-exporter does not advertise devices, so it never waits for partition-ready",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
				}},
			},
			args: args{
				component:     consts.RBLNMetricExporterName,
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{sleepCallCount: 0},
		},
		"LabelRemovedDuringWaitUnblocks": {
			reason: "labels are re-read every poll: aborting partitioning drops the partition-ready requirement",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
					{labels: map[string]string{}},
				}},
			},
			args: args{
				component:     consts.RBLNDevicePluginName,
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{sleepCallCount: 2},
		},
		"APIErrorRetriesWithoutDefaulting": {
			reason: "an API failure must not be treated as an unpartitioned node; the gate retries",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{err: errAPI},
					{labels: map[string]string{}},
				}},
			},
			args: args{
				component:     consts.RBLNFeatureDiscoveryName,
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{sleepCallCount: 1},
		},
		"InvalidLabelValueBlocksUntilFixed": {
			reason: "vf2 is not a supported partition mode; the gate blocks until the admin fixes the label",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{consts.RBLNPartitionLabelKey: "vf2"}},
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
				}},
			},
			args: args{
				component:     consts.RBLNDevicePluginName,
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile, consts.PartitionReadyFileName},
			},
			want: want{sleepCallCount: 1},
		},
		"UnknownComponent": {
			reason: "a miswired component name must fail immediately instead of waiting forever",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{}},
				}},
			},
			args: args{
				component:     "no-such-component",
				nodeName:      "node-1",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{err: true, sleepCallCount: 0},
		},
		"MissingNodeName": {
			reason: "a missing NODE_NAME env is a wiring bug and must fail fast",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{}},
				}},
			},
			args: args{
				component:     consts.RBLNDevicePluginName,
				nodeName:      "",
				existingFiles: []string{toolkitReadyFile},
			},
			want: want{err: true, sleepCallCount: 0},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			outputDir := t.TempDir()

			createFile := func(fileName string) {
				if err := os.WriteFile(filepath.Join(outputDir, fileName), nil, 0o600); err != nil {
					t.Fatalf("create file %s: %v", fileName, err)
				}
			}
			for _, f := range tc.args.existingFiles {
				createFile(f)
			}

			cfg := &gateConfig{
				outputDir:            outputDir,
				sleepIntervalSeconds: 1,
				component:            tc.args.component,
				nodeName:             tc.args.nodeName,
			}

			sleepCalls := 0
			rt := gateRuntime{
				getNodeLabels: tc.fields.getter.get,
				sleep: func(time.Duration) {
					sleepCalls++
					for _, f := range tc.args.createOnSleeps[sleepCalls] {
						createFile(f)
					}
				},
			}

			err := runGate(context.Background(), cfg, rt)

			if tc.want.err {
				if err == nil {
					t.Fatalf("%s: expected error, got nil", tc.reason)
				}
			} else if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}

			if sleepCalls != tc.want.sleepCallCount {
				t.Fatalf("%s: sleepCalls = %d, want %d", tc.reason, sleepCalls, tc.want.sleepCallCount)
			}
		})
	}
}

func TestComponentGateTableCoversAllGatedOperands(t *testing.T) {
	wantComponents := []string{
		consts.RBLNDevicePluginName,
		consts.RBLNDRAKubeletPluginName,
		consts.RBLNMetricExporterName,
		consts.RBLNFeatureDiscoveryName,
	}
	if len(componentWaitsForPartition) != len(wantComponents) {
		t.Fatalf("gate table has %d entries, want %d", len(componentWaitsForPartition), len(wantComponents))
	}
	for _, c := range wantComponents {
		if _, ok := componentWaitsForPartition[c]; !ok {
			t.Fatalf("gate table is missing component %q", c)
		}
	}
}

// DRA serves both workload types. Partition gates must not strand VM nodes
// waiting for toolkit-ready, or let container nodes advertise unpartitioned PFs.
func TestRunGateDRAWorkloads(t *testing.T) {
	for _, tc := range []struct {
		name          string
		workload      string
		toolkitReady  bool
		wantVFIOCalls int
	}{
		{name: "ContainerWaitsForPartition", workload: consts.RBLNWorkloadConfigContainer, toolkitReady: true},
		{name: "VMWaitsForVFIOWithoutToolkit", workload: consts.RBLNWorkloadConfigVMPassthrough, wantVFIOCalls: 2},
		{name: "VMDoesNotTrustStaleToolkitMarker", workload: consts.RBLNWorkloadConfigVMPassthrough, toolkitReady: true, wantVFIOCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputDir := t.TempDir()
			writeMarker := func(name string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(outputDir, name), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.toolkitReady {
				writeMarker(toolkitReadyFile)
			}
			sleeps, vfioCalls := 0, 0
			rt := gateRuntime{
				getNodeLabels: func(context.Context, string) (map[string]string, error) {
					return map[string]string{
						consts.RBLNWorkloadConfigLabelKey: tc.workload,
						consts.RBLNPartitionLabelKey:      consts.RBLNPartitionModeVF4,
					}, nil
				},
				validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					vfioCalls++
					if tc.workload != consts.RBLNWorkloadConfigVMPassthrough {
						t.Fatal("container DRA must wait for partition-ready without probing VFIO")
					}
					if sleeps == 0 {
						return vfiovalidator.Result{}, errors.New("VFIO binding not ready")
					}
					return vfiovalidator.Result{BoundDevices: []string{"0000:01:00.0"}}, nil
				},
				sleep: func(time.Duration) {
					sleeps++
					if sleeps > 1 {
						t.Fatal("DRA gate did not pass once its workload prerequisites were ready")
					}
					if tc.workload == consts.RBLNWorkloadConfigContainer {
						writeMarker(consts.PartitionReadyFileName)
					}
				},
			}
			cfg := &gateConfig{outputDir: outputDir, sleepIntervalSeconds: 1, component: consts.RBLNDRAKubeletPluginName, nodeName: "node-1"}
			if err := runGate(context.Background(), cfg, rt); err != nil {
				t.Fatal(err)
			}
			if sleeps != 1 || vfioCalls != tc.wantVFIOCalls {
				t.Fatalf("sleeps=%d vfioCalls=%d, want sleeps=1 vfioCalls=%d", sleeps, vfioCalls, tc.wantVFIOCalls)
			}
		})
	}
}
