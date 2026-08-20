package v1beta1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

var (
	testEnv   *envtest.Environment
	testCfg   *rest.Config
	testK8s   client.Client
	testSetup bool
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if err := startEnvtest(); err != nil {
		fmt.Fprintf(os.Stderr, "envtest setup failed (tests will skip): %v\n", err)
		return m.Run()
	}
	defer func() {
		if testEnv != nil {
			_ = testEnv.Stop()
		}
	}()
	return m.Run()
}

func startEnvtest() error {
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	// BinaryAssetsDirectory is intentionally unset so envtest honors
	// KUBEBUILDER_ASSETS, which `make unit-tests` provides.

	cfg, err := testEnv.Start()
	if err != nil {
		return fmt.Errorf("start envtest: %w", err)
	}
	if err := rblnv1beta1.AddToScheme(scheme.Scheme); err != nil {
		return fmt.Errorf("register scheme: %w", err)
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	testCfg = cfg
	testK8s = k8s
	testSetup = true
	return nil
}

func requireEnvtest(t *testing.T) {
	t.Helper()
	if !testSetup {
		t.Skip("envtest environment not available; skipping (run via `make unit-tests`)")
	}
}

// TestRBLNClusterPolicy_DefaultsApplied verifies that submitting a minimal CR
// with only required nested objects present results in the expected default
// values being populated by the API server from the CRD schema.
//
// An unstructured spec is used so the client does not serialize Go zero values
// (e.g. `workloadType: ""`, `enabled: false`) that would otherwise short-circuit
// default application.
func TestRBLNClusterPolicy_DefaultsApplied(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	emptyNested := map[string]any{}
	cp := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "rebellions.ai/v1beta1",
			"kind":       "RBLNClusterPolicy",
			"metadata":   map[string]any{"name": "defaults-test"},
			"spec": map[string]any{
				"vfioManager":         emptyNested,
				"sandboxDevicePlugin": emptyNested,
				"devicePlugin":        emptyNested,
				"metricsExporter":     emptyNested,
				"npuFeatureDiscovery": emptyNested,
				"containerToolkit":    emptyNested,
				"driver":              emptyNested,
			},
		},
	}
	cp.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "rebellions.ai", Version: "v1beta1", Kind: "RBLNClusterPolicy",
	})

	if err := testK8s.Create(ctx, cp); err != nil {
		t.Fatalf("create minimal CR: %v", err)
	}
	t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })

	got := &rblnv1beta1.RBLNClusterPolicy{}
	if err := testK8s.Get(ctx, client.ObjectKey{Name: "defaults-test"}, got); err != nil {
		t.Fatalf("get CR back: %v", err)
	}

	if got.Spec.WorkloadType != "container" {
		t.Errorf("WorkloadType default: want %q, got %q", "container", got.Spec.WorkloadType)
	}
	if got.Spec.DevicePlugin.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("DevicePlugin.ImagePullPolicy default: want %q, got %q", corev1.PullIfNotPresent, got.Spec.DevicePlugin.ImagePullPolicy)
	}
	if got.Spec.ContainerToolkit.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("ContainerToolkit.ImagePullPolicy default: want %q, got %q", corev1.PullIfNotPresent, got.Spec.ContainerToolkit.ImagePullPolicy)
	}
	if !got.Spec.DevicePlugin.Enabled {
		t.Errorf("DevicePlugin.Enabled default: want true, got false")
	}
}

// TestRBLNClusterPolicy_CELRejection verifies that the XValidation rules
// reject inconsistent workloadType/component combinations at admission time.
func TestRBLNClusterPolicy_CELRejection(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	cases := map[string]struct {
		spec      rblnv1beta1.RBLNClusterPolicySpec
		errSubstr string
	}{
		"vm-passthrough without vfioManager": {
			spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType:        "vm-passthrough",
				VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: false},
				SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: true},
				DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{},
				MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
				NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
				ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
				Driver:              rblnv1beta1.DriverSpec{},
			},
			errSubstr: "vfioManager.enabled must be true",
		},
		"vm-passthrough without any vfio consumer": {
			spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType:        "vm-passthrough",
				VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: true},
				SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
				DRAKubeletPlugin:    rblnv1beta1.RBLNDRAKubeletPluginSpec{Enabled: false},
				DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{},
				MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
				NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
				ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
				Driver:              rblnv1beta1.DriverSpec{},
			},
			errSubstr: "either sandboxDevicePlugin.enabled or draKubeletPlugin.enabled must be true",
		},
		"sandboxDevicePlugin and draKubeletPlugin both enabled": {
			spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType:        "vm-passthrough",
				VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: true},
				SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: true},
				DRAKubeletPlugin:    rblnv1beta1.RBLNDRAKubeletPluginSpec{Enabled: true},
				DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{},
				MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
				NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
				ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
				Driver:              rblnv1beta1.DriverSpec{},
			},
			errSubstr: "mutually exclusive",
		},
		"devicePlugin and draKubeletPlugin both enabled": {
			spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType:        "container",
				VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{},
				SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{},
				DRAKubeletPlugin:    rblnv1beta1.RBLNDRAKubeletPluginSpec{Enabled: true},
				DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{Enabled: true},
				MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
				NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
				ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
				Driver:              rblnv1beta1.DriverSpec{},
			},
			errSubstr: "devicePlugin.enabled and draKubeletPlugin.enabled are mutually exclusive",
		},
		"invalid pull policy enum": {
			spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType: "container",
				DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
					PodSpec: rblnv1beta1.PodSpec{ImagePullPolicy: "Sometimes"},
				},
				VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{},
				SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{},
				MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
				NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
				ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
				Driver:              rblnv1beta1.DriverSpec{},
			},
			errSubstr: "Unsupported value",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cp := &rblnv1beta1.RBLNClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-" + strings.ToLower(strings.ReplaceAll(name, " ", "-"))},
				Spec:       tc.spec,
			}
			err := testK8s.Create(ctx, cp)
			if err == nil {
				_ = testK8s.Delete(ctx, cp)
				t.Fatalf("expected admission rejection, create succeeded")
			}
			if !strings.Contains(err.Error(), tc.errSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.errSubstr, err)
			}
		})
	}
}

func TestRBLNClusterPolicy_CELAcceptsDRAPassthrough(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	cp := &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-dra-passthrough"},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType:        "vm-passthrough",
			VFIOManager:         rblnv1beta1.RBLNVFIOManagerSpec{Enabled: true},
			SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
			DRAKubeletPlugin:    rblnv1beta1.RBLNDRAKubeletPluginSpec{Enabled: true},
			DevicePlugin:        rblnv1beta1.RBLNDevicePluginSpec{},
			MetricsExporter:     rblnv1beta1.RBLNMetricsExporterSpec{},
			NPUFeatureDiscovery: rblnv1beta1.RBLNNPUFeatureDiscoverySpec{},
			ContainerToolkit:    rblnv1beta1.RBLNContainerToolkitSpec{},
			Driver:              rblnv1beta1.DriverSpec{},
		},
	}
	if err := testK8s.Create(ctx, cp); err != nil {
		t.Fatalf("expected admission to accept sandbox-off + DRA-on vm-passthrough spec: %v", err)
	}
	t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })
}
