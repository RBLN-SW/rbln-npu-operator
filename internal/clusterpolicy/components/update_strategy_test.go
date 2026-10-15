package components

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// Exercise every component's real reconciliation path: an omitted setting
// must preserve manual overrides, and explicit settings must change only the
// DaemonSet strategy, never the pod template or another component's policy.
func TestComponentUpdateStrategies(t *testing.T) {
	tests := map[string]struct {
		newPatcher func(client.Client, logr.Logger, string, *rblnv1beta1.RBLNClusterPolicySpec, *runtime.Scheme, string) Patcher
		strategy   func(*rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec
	}{
		"device plugin": {NewDevicePluginPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.DevicePlugin.UpdateStrategy
		}},
		"dra kubelet plugin": {NewDRAKubeletPluginPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.DRAKubeletPlugin.UpdateStrategy
		}},
		"sandbox device plugin": {NewSandboxDevicePluginPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.SandboxDevicePlugin.UpdateStrategy
		}},
		"metrics exporter": {NewMetricsExporterPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.MetricsExporter.UpdateStrategy
		}},
		"npu feature discovery": {NewNPUFeatureDiscoveryPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.NPUFeatureDiscovery.UpdateStrategy
		}},
		"vfio manager": {NewVFIOManagerPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.VFIOManager.UpdateStrategy
		}},
		"validator": {NewValidatorPatcher, func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
			return &s.Validator.UpdateStrategy
		}},
		"container toolkit": {
			func(c client.Client, l logr.Logger, ns string, s *rblnv1beta1.RBLNClusterPolicySpec, sc *runtime.Scheme, os string) Patcher {
				return NewContainerToolkitPatcher(c, l, ns, s, sc, os, "containerd")
			},
			func(s *rblnv1beta1.RBLNClusterPolicySpec) **rblnv1beta1.DaemonSetUpdateStrategySpec {
				return &s.ContainerToolkit.UpdateStrategy
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			scheme := newTestScheme(t)
			c := newFakeClient(t, scheme)
			owner := newTestOwner()
			owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{Enabled: true, Image: "device-plugin", Version: "v1"}
			owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: true, Image: "sandbox-plugin", Version: "v1"}
			owner.Spec.MetricsExporter = rblnv1beta1.RBLNMetricsExporterSpec{Enabled: true, Image: "metrics-exporter", Version: "v1"}
			owner.Spec.NPUFeatureDiscovery = rblnv1beta1.RBLNNPUFeatureDiscoverySpec{Enabled: true, Image: "feature-discovery", Version: "v1"}
			owner.Spec.VFIOManager = rblnv1beta1.RBLNVFIOManagerSpec{Enabled: true, Image: "vfio-manager", Version: "v1"}
			owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{Enabled: true, Image: "dra-plugin", Version: "v1", DriverName: "npu.rebellions.ai"}

			patch := func() *appsv1.DaemonSet {
				t.Helper()
				p := tc.newPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
				if err := p.Patch(ctx, owner); err != nil {
					t.Fatalf("Patch(): %v", err)
				}
				ds := &appsv1.DaemonSet{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: p.ComponentName()}, ds); err != nil {
					t.Fatal(err)
				}
				return ds
			}
			ds := patch()
			// The fake client does not default objects: zero here means the API
			// server remains responsible for defaults, as before this feature.
			if !reflect.DeepEqual(ds.Spec.UpdateStrategy, appsv1.DaemonSetUpdateStrategy{}) {
				t.Fatalf("unset strategy was rendered: %+v", ds.Spec.UpdateStrategy)
			}
			template := ds.Spec.Template.DeepCopy()

			for _, manual := range []appsv1.DaemonSetUpdateStrategy{
				{Type: appsv1.OnDeleteDaemonSetStrategyType},
				rollingStrategy(intstr.FromString("20%")),
			} {
				ds.Spec.UpdateStrategy = manual
				if err := c.Update(ctx, ds); err != nil {
					t.Fatal(err)
				}
				before := ds.ResourceVersion
				ds = patch()
				if !reflect.DeepEqual(ds.Spec.UpdateStrategy, manual) || ds.ResourceVersion != before {
					t.Fatalf("unset strategy changed a legacy DaemonSet: %+v", ds.Spec.UpdateStrategy)
				}
			}

			percent := intstr.FromString("10%")
			count := intstr.FromInt32(3)
			steps := []struct {
				name string
				set  *rblnv1beta1.DaemonSetUpdateStrategySpec
				want appsv1.DaemonSetUpdateStrategy
			}{
				{"explicit defaults", &rblnv1beta1.DaemonSetUpdateStrategySpec{}, rollingStrategy(intstr.FromInt32(1))},
				{"percentage", &rblnv1beta1.DaemonSetUpdateStrategySpec{RollingUpdate: &rblnv1beta1.RollingUpdateDaemonSetSpec{MaxUnavailable: &percent}}, rollingStrategy(percent)},
				{"count", &rblnv1beta1.DaemonSetUpdateStrategySpec{RollingUpdate: &rblnv1beta1.RollingUpdateDaemonSetSpec{MaxUnavailable: &count}}, rollingStrategy(count)},
				{"on delete", &rblnv1beta1.DaemonSetUpdateStrategySpec{Type: appsv1.OnDeleteDaemonSetStrategyType}, appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}},
				{"remove preserves on delete", nil, appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}},
				{"reset to rolling", &rblnv1beta1.DaemonSetUpdateStrategySpec{Type: appsv1.RollingUpdateDaemonSetStrategyType}, rollingStrategy(intstr.FromInt32(1))},
			}
			for _, step := range steps {
				*tc.strategy(&owner.Spec) = step.set
				ds = patch()
				if !reflect.DeepEqual(ds.Spec.UpdateStrategy, step.want) {
					t.Fatalf("%s: strategy = %+v, want %+v", step.name, ds.Spec.UpdateStrategy, step.want)
				}
				if !reflect.DeepEqual(&ds.Spec.Template, template) {
					t.Fatalf("%s changed the pod template", step.name)
				}
				before := ds.ResourceVersion
				if got := patch(); got.ResourceVersion != before {
					t.Fatalf("%s: repeated reconcile changed the DaemonSet", step.name)
				}
			}
		})
	}
}

func rollingStrategy(maxUnavailable intstr.IntOrString) appsv1.DaemonSetUpdateStrategy {
	zero := intstr.FromInt32(0)
	return appsv1.DaemonSetUpdateStrategy{
		Type: appsv1.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDaemonSet{
			MaxUnavailable: &maxUnavailable,
			MaxSurge:       &zero,
		},
	}
}
