package components

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// The operand images do not share one variable name -- the metrics exporter
// reads the unprefixed pair -- so each component is asserted against the names
// its own image actually reads.
func TestLoggingSpecRendersOperandEnv(t *testing.T) {
	tests := map[string]struct {
		component string
		newPatch  func(client.Client, *rblnv1beta1.RBLNClusterPolicySpec, *runtime.Scheme) Patcher
		apply     func(*rblnv1beta1.RBLNClusterPolicySpec, *rblnv1beta1.LoggingSpec)
		levelEnv  string
		formatEnv string
	}{
		"device plugin": {
			component: consts.RBLNDevicePluginName,
			newPatch: func(c client.Client, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme) Patcher {
				return NewDevicePluginPatcher(c, logf.Log, testNamespace, s, sc, "")
			},
			apply: func(s *rblnv1beta1.RBLNClusterPolicySpec, l *rblnv1beta1.LoggingSpec) {
				s.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
					Enabled: true, Registry: "docker.io", Image: "rebellions/k8s-device-plugin",
					Version: "latest", Logging: l,
				}
			},
			levelEnv:  devicePluginLogLevelEnv,
			formatEnv: devicePluginLogFormatEnv,
		},
		"metrics exporter": {
			component: consts.RBLNMetricExporterName,
			newPatch: func(c client.Client, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme) Patcher {
				return NewMetricsExporterPatcher(c, logf.Log, testNamespace, s, sc, "")
			},
			apply: func(s *rblnv1beta1.RBLNClusterPolicySpec, l *rblnv1beta1.LoggingSpec) {
				s.MetricsExporter = rblnv1beta1.RBLNMetricsExporterSpec{
					Enabled: true, Registry: "docker.io", Image: "rebellions/metrics-exporter",
					Version: "latest", Logging: l,
				}
			},
			levelEnv:  metricsExporterLogLevelEnv,
			formatEnv: metricsExporterLogFormatEnv,
		},
		"npu feature discovery": {
			component: consts.RBLNFeatureDiscoveryName,
			newPatch: func(c client.Client, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme) Patcher {
				return NewNPUFeatureDiscoveryPatcher(c, logf.Log, testNamespace, s, sc, "")
			},
			apply: func(s *rblnv1beta1.RBLNClusterPolicySpec, l *rblnv1beta1.LoggingSpec) {
				s.NPUFeatureDiscovery = rblnv1beta1.RBLNNPUFeatureDiscoverySpec{
					Enabled: true, Registry: "docker.io", Image: "rebellions/npu-feature-discovery",
					Version: "latest", Logging: l,
				}
			},
			levelEnv:  nfdLogLevelEnv,
			formatEnv: nfdLogFormatEnv,
		},
		"dra kubelet plugin": {
			component: consts.RBLNDRAKubeletPluginName,
			newPatch: func(c client.Client, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme) Patcher {
				return NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, s, sc, "")
			},
			apply: func(s *rblnv1beta1.RBLNClusterPolicySpec, l *rblnv1beta1.LoggingSpec) {
				s.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
					Enabled: true, Registry: "docker.io", Image: "rebellions/k8s-dra-driver-npu",
					Version: "latest", DriverName: "npu.rebellions.ai",
					KubeletRegistrarDirectoryPath: "/var/lib/kubelet/plugins_registry",
					KubeletPluginsDirectoryPath:   "/var/lib/kubelet/plugins",
					HealthcheckPort:               51515, Logging: l,
				}
			},
			levelEnv:  draLogLevelEnv,
			formatEnv: draLogFormatEnv,
		},
		"sandbox device plugin": {
			component: consts.RBLNSandboxDevicePluginName,
			newPatch: func(c client.Client, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme) Patcher {
				return NewSandboxDevicePluginPatcher(c, logf.Log, testNamespace, s, sc, "")
			},
			apply: func(s *rblnv1beta1.RBLNClusterPolicySpec, l *rblnv1beta1.LoggingSpec) {
				s.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
					Enabled: true, Registry: "docker.io", Image: "rebellions/sandbox-device-plugin",
					Version: "dev", Logging: l,
				}
			},
			levelEnv:  sandboxDevicePluginLogLevelEnv,
			formatEnv: sandboxDevicePluginLogFormatEnv,
		},
	}

	for name, tc := range tests {
		t.Run(name+" renders the configured gate", func(t *testing.T) {
			env := patchAndReadMainContainerEnv(t, tc.component, tc.newPatch, func(s *rblnv1beta1.RBLNClusterPolicySpec) {
				tc.apply(s, &rblnv1beta1.LoggingSpec{Level: "debug", Format: "text"})
			})
			if got := envValue(env, tc.levelEnv); got != "debug" {
				t.Errorf("%s = %q, want %q", tc.levelEnv, got, "debug")
			}
			if got := envValue(env, tc.formatEnv); got != "text" {
				t.Errorf("%s = %q, want %q", tc.formatEnv, got, "text")
			}
		})

		// An unset block must stay off the DaemonSet: restating the operand's
		// own default would churn the pod template for no behavior change.
		t.Run(name+" renders nothing when unset", func(t *testing.T) {
			env := patchAndReadMainContainerEnv(t, tc.component, tc.newPatch, func(s *rblnv1beta1.RBLNClusterPolicySpec) {
				tc.apply(s, nil)
			})
			for _, e := range env {
				if e.Name == tc.levelEnv || e.Name == tc.formatEnv {
					t.Errorf("unset logging must not render %s", e.Name)
				}
			}
		})
	}
}

func patchAndReadMainContainerEnv(
	t *testing.T,
	component string,
	newPatch func(client.Client, *rblnv1beta1.RBLNClusterPolicySpec, *runtime.Scheme) Patcher,
	apply func(*rblnv1beta1.RBLNClusterPolicySpec),
) []corev1.EnvVar {
	t.Helper()

	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newTestOwner()
	apply(&owner.Spec)

	if err := newPatch(c, &owner.Spec, scheme).Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	ds := assertDaemonSetBasics(t, c, consts.RBLNBaseName+"-"+component, owner.Name)
	return ds.Spec.Template.Spec.Containers[0].Env
}
