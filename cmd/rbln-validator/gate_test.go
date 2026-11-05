package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	vfiovalidator "github.com/RBLN-SW/rbln-npu-operator/cmd/rbln-validator/vfiopci"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
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
		"PartitionManagerNeverWaitsForPartitionReady": {
			reason: "the partition-manager waits only for the driver, never toolkit or its own partition-ready",
			fields: fields{
				getter: &fakeNodeLabelsGetter{results: []nodeLabelsResult{
					{labels: map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}},
				}},
			},
			args: args{
				component:     consts.RBLNPartitionManagerName,
				nodeName:      "node-1",
				existingFiles: nil,
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
				checkDriver:        func(context.Context) error { return nil },
				checkBindingDriver: func(context.Context) error { return nil },
				getNodeLabels:      tc.fields.getter.get,
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

func TestGateRejectsStaleToolkitMarkerUntilDriverRecovers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, toolkitReadyFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	checks, sleeps := 0, 0
	rt := gateRuntime{
		getNodeLabels: func(context.Context, string) (map[string]string, error) { return nil, nil },
		checkDriver: func(context.Context) error {
			checks++
			if checks <= 2 {
				return errors.New("driver not ready or API unavailable")
			}
			return nil
		},
		sleep: func(time.Duration) { sleeps++ },
	}
	cfg := &gateConfig{nodeName: "node", outputDir: dir, component: consts.RBLNDevicePluginName}
	if err := runGate(context.Background(), cfg, rt); err != nil {
		t.Fatal(err)
	}
	if sleeps != 2 || checks != 3 {
		t.Fatalf("stale toolkit marker bypassed current driver: checks=%d sleeps=%d", checks, sleeps)
	}
}

func TestComponentGateTableCoversAllGatedOperands(t *testing.T) {
	wantComponents := []string{
		consts.RBLNSandboxDevicePluginName,
		consts.RBLNDevicePluginName,
		consts.RBLNDRAKubeletPluginName,
		consts.RBLNMetricExporterName,
		consts.RBLNFeatureDiscoveryName,
		consts.RBLNPartitionManagerName,
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
		name            string
		workload        string
		defaultWorkload string
		toolkitReady    bool
		wantVFIOCalls   int
	}{
		{name: "ContainerWaitsForPartition", workload: consts.RBLNWorkloadConfigContainer, toolkitReady: true},
		{
			name: "VMDefaultWithoutNodeLabel", defaultWorkload: consts.RBLNWorkloadConfigVMPassthrough,
			wantVFIOCalls: 2,
		},
		{
			name: "VMDefaultWithInvalidNodeLabel", workload: "invalid",
			defaultWorkload: consts.RBLNWorkloadConfigVMPassthrough, wantVFIOCalls: 2,
		},
		{
			name: "ContainerLabelOverridesVMDefault", workload: consts.RBLNWorkloadConfigContainer,
			defaultWorkload: consts.RBLNWorkloadConfigVMPassthrough, toolkitReady: true,
		},
		{
			name: "VMLabelOverridesContainerDefault", workload: consts.RBLNWorkloadConfigVMPassthrough,
			defaultWorkload: consts.RBLNWorkloadConfigContainer, wantVFIOCalls: 2,
		},
		{
			name: "ContainerDefaultWithoutNodeLabel", defaultWorkload: consts.RBLNWorkloadConfigContainer,
			toolkitReady: true,
		},
		{name: "VMWaitsForVFIOWithoutToolkit", workload: consts.RBLNWorkloadConfigVMPassthrough, wantVFIOCalls: 2},
		{
			name: "VMDoesNotTrustStaleToolkitMarker", workload: consts.RBLNWorkloadConfigVMPassthrough,
			toolkitReady: true, wantVFIOCalls: 2,
		},
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
				checkDriver: func(context.Context) error { return nil },
				getNodeLabels: func(context.Context, string) (map[string]string, error) {
					labels := map[string]string{consts.RBLNPartitionLabelKey: consts.RBLNPartitionModeVF4}
					if tc.workload != "" {
						labels[consts.RBLNWorkloadConfigLabelKey] = tc.workload
					}
					return labels, nil
				},
				validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					vfioCalls++
					if tc.wantVFIOCalls == 0 {
						t.Fatal("container DRA must wait for partition-ready without probing VFIO")
					}
					if vfioCalls == 1 {
						return vfiovalidator.Result{}, errors.New("VFIO binding not ready")
					}
					return vfiovalidator.Result{BoundDevices: []string{"0000:01:00.0"}}, nil
				},
				sleep: func(time.Duration) {
					sleeps++
					if sleeps > 2 {
						t.Fatal("DRA gate did not pass once its workload prerequisites were ready")
					}
					writeMarker(consts.PartitionReadyFileName)
				},
			}
			t.Setenv("NODE_NAME", "node-1")
			t.Setenv("DEFAULT_WORKLOAD_TYPE", tc.defaultWorkload)
			cfg := (&rootConfig{outputDir: outputDir, sleepIntervalSeconds: 1}).gateConfig(consts.RBLNDRAKubeletPluginName)
			if err := runGate(context.Background(), cfg, rt); err != nil {
				t.Fatal(err)
			}
			wantSleeps := 1
			if tc.wantVFIOCalls > 0 {
				wantSleeps = 2
			}
			if sleeps != wantSleeps || vfioCalls != tc.wantVFIOCalls {
				t.Fatalf("sleeps=%d vfioCalls=%d, want sleeps=%d vfioCalls=%d", sleeps, vfioCalls, wantSleeps, tc.wantVFIOCalls)
			}
		})
	}
}

func TestVMConsumersWaitForPartitionAndVFIO(t *testing.T) {
	for _, component := range []string{consts.RBLNDRAKubeletPluginName, consts.RBLNSandboxDevicePluginName} {
		t.Run(component, func(t *testing.T) {
			dir := t.TempDir()
			calls, sleeps := 0, 0
			rt := gateRuntime{
				getNodeLabels: func(context.Context, string) (map[string]string, error) {
					mode := consts.RBLNPartitionModeVF1
					if sleeps == 0 {
						mode = "invalid"
					}
					return map[string]string{
						consts.RBLNWorkloadConfigLabelKey: consts.RBLNWorkloadConfigVMPassthrough,
						consts.RBLNPartitionLabelKey:      mode,
					}, nil
				},
				checkDriver: func(context.Context) error { t.Fatal("VM gate must not wait for the container stack"); return nil },
				validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					calls++
					if calls == 1 {
						return vfiovalidator.Result{}, errors.New("partial VF binding")
					}
					return vfiovalidator.Result{BoundDevices: []string{"VF"}}, nil
				},
				sleep: func(time.Duration) {
					sleeps++
					if sleeps <= 2 && calls != 0 {
						t.Fatal("probed VFIO before label validation and partition-ready")
					}
					if sleeps == 2 {
						if err := os.WriteFile(filepath.Join(dir, consts.PartitionReadyFileName), nil, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if sleeps > 3 {
						t.Fatal("gate did not converge")
					}
				},
			}
			cfg := &gateConfig{nodeName: "node", component: component, outputDir: dir}
			if err := runGate(context.Background(), cfg, rt); err != nil {
				t.Fatal(err)
			}
			if sleeps != 3 || calls != 2 {
				t.Fatalf("sleeps=%d calls=%d", sleeps, calls)
			}
		})
	}
}

func TestContainerPFConsumerWaitsForBindingReconcile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, toolkitReadyFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	sleeps := 0
	rt := gateRuntime{
		getNodeLabels: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"rebellions.ai/npu.deploy.partition-manager": "true"}, nil
		},
		checkDriver: func(context.Context) error { return nil },
		sleep: func(time.Duration) {
			sleeps++
			if sleeps > 1 {
				t.Fatal("container PF gate did not converge")
			}
			if err := os.WriteFile(filepath.Join(dir, consts.PartitionReadyFileName), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}
	cfg := &gateConfig{nodeName: "node", component: consts.RBLNDevicePluginName, outputDir: dir}
	if err := runGate(context.Background(), cfg, rt); err != nil {
		t.Fatal(err)
	}
	if sleeps != 1 {
		t.Fatal("PF-only node bypassed manager binding readiness")
	}
}

func TestPartitionManagerInitWaitsOnlyForDriver(t *testing.T) {
	for _, workload := range []string{consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough} {
		t.Run(workload, func(t *testing.T) {
			checks, sleeps := 0, 0
			cfg := &gateConfig{
				nodeName:        "node",
				component:       consts.RBLNPartitionManagerName,
				defaultWorkload: workload,
				outputDir:       t.TempDir(),
			}
			rt := gateRuntime{
				checkBindingDriver: func(context.Context) error {
					checks++
					if checks < 3 {
						return errors.New("driver installing")
					}
					return nil
				},
				getNodeLabels: func(context.Context, string) (map[string]string, error) {
					t.Fatal("driver gate must not wait on partition labels")
					return nil, nil
				},
				checkDriver: func(context.Context) error { t.Fatal("must use host-aware driver check"); return nil },
				validateVFIO: func(vfiovalidator.Config) (vfiovalidator.Result, error) {
					t.Fatal("driver gate cannot wait for VFIO binding")
					return vfiovalidator.Result{}, nil
				},
				sleep: func(time.Duration) { sleeps++ },
			}
			if err := runGate(context.Background(), cfg, rt); err != nil {
				t.Fatal(err)
			}
			if checks != 3 || sleeps != 2 {
				t.Fatalf("checks=%d sleeps=%d", checks, sleeps)
			}
		})
	}
}

func TestPolicyBackedGateRetriesAndReadsCurrentDefault(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, consts.PartitionReadyFileName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	calls, sleeps := 0, 0
	rt := gateRuntime{
		getNodeLabels: func(context.Context, string) (map[string]string, error) { return map[string]string{}, nil },
		getDefaultWorkload: func(context.Context, string) (string, error) {
			calls++
			if calls == 1 {
				return "", errors.New("API unavailable")
			}
			if calls == 2 {
				return "container", nil
			}
			return "vm-passthrough", nil
		},
		checkDriver:        func(context.Context) error { return nil },
		checkBindingDriver: func(context.Context) error { return nil },
		validateVFIO:       func(vfiovalidator.Config) (vfiovalidator.Result, error) { return vfiovalidator.Result{}, nil },
		sleep: func(time.Duration) {
			sleeps++
			if sleeps > 3 {
				t.Fatal("gate did not observe updated policy")
			}
		},
	}
	cfg := &gateConfig{
		component: consts.RBLNDRAKubeletPluginName, nodeName: "node", policyName: "policy", outputDir: dir,
	}
	err := runGate(context.Background(), cfg, rt)
	if err != nil || calls != 3 || sleeps != 2 {
		t.Fatalf("calls=%d sleeps=%d error=%v", calls, sleeps, err)
	}
}

func TestExplicitWorkloadLabelDoesNotRequirePolicyDefault(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, consts.PartitionReadyFileName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	rt := gateRuntime{
		getNodeLabels: func(context.Context, string) (map[string]string, error) {
			return map[string]string{consts.RBLNWorkloadConfigLabelKey: "vm-passthrough"}, nil
		},
		getDefaultWorkload: func(context.Context, string) (string, error) {
			t.Fatal("explicit label must take precedence")
			return "", nil
		},
		checkBindingDriver: func(context.Context) error { return nil },
		validateVFIO:       func(vfiovalidator.Config) (vfiovalidator.Result, error) { return vfiovalidator.Result{}, nil },
	}
	cfg := &gateConfig{
		component: consts.RBLNDRAKubeletPluginName, nodeName: "node", policyName: "policy", outputDir: dir,
	}
	if err := runGate(context.Background(), cfg, rt); err != nil {
		t.Fatal(err)
	}
}
