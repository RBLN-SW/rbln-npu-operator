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

package v1beta1_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
)

var strategyComponents = []string{
	"devicePlugin", "draKubeletPlugin", "sandboxDevicePlugin", "metricsExporter",
	"npuFeatureDiscovery", "containerToolkit", "vfioManager", "validator",
}

func newUpdateStrategyCR(name string) *unstructured.Unstructured {
	spec := map[string]any{"driver": map[string]any{}}
	for _, component := range strategyComponents {
		if component == "validator" {
			spec[component] = map[string]any{"image": "rbln-validator", "version": "test"}
		} else {
			spec[component] = map[string]any{"enabled": false}
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rebellions.ai/v1beta1", "kind": "RBLNClusterPolicy",
		"metadata": map[string]any{"name": name}, "spec": spec,
	}}
}

// Admission must keep legacy CRs unset, accept settings on every supported
// component, and reject policies that would fail later at DaemonSet admission.
func TestUpdateStrategyAdmission(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	for _, mode := range []string{"omitted", "null"} {
		t.Run(mode+" preserves legacy behavior", func(t *testing.T) {
			legacy := newUpdateStrategyCR("strategy-legacy-" + mode)
			if mode == "null" {
				for _, component := range strategyComponents {
					if err := unstructured.SetNestedField(legacy.Object, nil, "spec", component, "updateStrategy"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := testK8s.Create(ctx, legacy); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = testK8s.Delete(ctx, legacy) })
			for _, component := range strategyComponents {
				if _, found, err := unstructured.NestedFieldNoCopy(legacy.Object, "spec", component, "updateStrategy"); err != nil || found {
					t.Fatalf("legacy %s gained an updateStrategy: found=%v err=%v", component, found, err)
				}
			}
		})
	}

	cases := []struct {
		name     string
		strategy map[string]any
		invalid  bool
	}{
		{"defaults", map[string]any{}, false},
		{"rolling-defaults", map[string]any{"rollingUpdate": map[string]any{}}, false},
		{"count", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": int64(3)}}, false},
		{"percent", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "10%"}}, false},
		{"all", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "100%"}}, false},
		{"ondelete", map[string]any{"type": "OnDelete"}, false},
		{"ondelete-null-rolling", map[string]any{"type": "OnDelete", "rollingUpdate": nil}, false},
		{"bad-type", map[string]any{"type": "Recreate"}, true},
		{"ondelete-rolling", map[string]any{"type": "OnDelete", "rollingUpdate": map[string]any{}}, true},
		{"zero", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": int64(0)}}, true},
		{"negative", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": int64(-1)}}, true},
		{"zero-percent", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "0%"}}, true},
		{"over-percent", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "101%"}}, true},
		{"string-count", map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "3"}}, true},
	}
	for _, component := range strategyComponents {
		for _, tc := range cases {
			t.Run(component+"/"+tc.name, func(t *testing.T) {
				cp := newUpdateStrategyCR("strategy-" + strings.ToLower(component) + "-" + tc.name)
				if err := unstructured.SetNestedMap(cp.Object, tc.strategy, "spec", component, "updateStrategy"); err != nil {
					t.Fatal(err)
				}
				err := testK8s.Create(ctx, cp)
				if tc.invalid {
					if !apierrors.IsInvalid(err) {
						t.Fatalf("expected validation rejection, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })
				gotType, _, _ := unstructured.NestedString(cp.Object, "spec", component, "updateStrategy", "type")
				wantType := "RollingUpdate"
				if tc.strategy["type"] == "OnDelete" {
					wantType = "OnDelete"
				}
				if gotType != wantType {
					t.Fatalf("type = %q, want %q", gotType, wantType)
				}
				if tc.name == "rolling-defaults" {
					got, _, _ := unstructured.NestedInt64(cp.Object, "spec", component, "updateStrategy", "rollingUpdate", "maxUnavailable")
					if got != 1 {
						t.Fatalf("maxUnavailable default = %d, want 1", got)
					}
				}
			})
		}
	}
}

// The real API catches invalid strategy transitions and defaulting-induced
// patch loops that a fake client cannot detect. This also verifies that adding
// the feature to an existing installation never changes the pod template.
func TestUpdateStrategyAPIServerTransitions(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "strategy-upgrade"}}
	if err := testK8s.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testK8s.Delete(ctx, ns) })
	cp := newUpdateStrategyCR("strategy-transitions")
	if err := unstructured.SetNestedField(cp.Object, true, "spec", "devicePlugin", "enabled"); err != nil {
		t.Fatal(err)
	}
	if err := testK8s.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })

	patch := func() *appsv1.DaemonSet {
		t.Helper()
		owner := &rblnv1beta1.RBLNClusterPolicy{}
		if err := testK8s.Get(ctx, client.ObjectKeyFromObject(cp), owner); err != nil {
			t.Fatal(err)
		}
		p := components.NewDevicePluginPatcher(testK8s, logr.Discard(), ns.Name, &owner.Spec, scheme.Scheme, "")
		if err := p.Patch(ctx, owner); err != nil {
			t.Fatal(err)
		}
		ds := &appsv1.DaemonSet{}
		if err := testK8s.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: p.ComponentName()}, ds); err != nil {
			t.Fatal(err)
		}
		return ds
	}
	ds := patch()
	template := ds.Spec.Template.DeepCopy()
	if ds.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType || ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.IntVal != 1 || ds.Spec.UpdateStrategy.RollingUpdate.MaxSurge.IntVal != 0 {
		t.Fatalf("legacy defaults changed: %+v", ds.Spec.UpdateStrategy)
	}

	// An administrator's pre-existing OnDelete override survives the upgrade.
	ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
	if err := testK8s.Update(ctx, ds); err != nil {
		t.Fatal(err)
	}
	before := ds.ResourceVersion
	if got := patch(); got.ResourceVersion != before {
		t.Fatal("legacy DaemonSet changed with the setting omitted")
	}

	for i, step := range []struct {
		strategy any
		wantType appsv1.DaemonSetUpdateStrategyType
		wantMax  string
	}{
		{map[string]any{"type": "RollingUpdate"}, appsv1.RollingUpdateDaemonSetStrategyType, "1"},
		{map[string]any{"rollingUpdate": map[string]any{"maxUnavailable": "10%"}}, appsv1.RollingUpdateDaemonSetStrategyType, "10%"},
		{map[string]any{"type": "OnDelete", "rollingUpdate": nil}, appsv1.OnDeleteDaemonSetStrategyType, ""},
		{nil, appsv1.OnDeleteDaemonSetStrategyType, ""},
		{map[string]any{"type": "RollingUpdate"}, appsv1.RollingUpdateDaemonSetStrategyType, "1"},
	} {
		body, err := json.Marshal(map[string]any{"spec": map[string]any{"devicePlugin": map[string]any{"updateStrategy": step.strategy}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := testK8s.Patch(ctx, cp, client.RawPatch(types.MergePatchType, body)); err != nil {
			t.Fatalf("step %d: patch policy: %v", i, err)
		}
		ds = patch()
		if ds.Spec.UpdateStrategy.Type != step.wantType {
			t.Fatalf("step %d: type = %s, want %s", i, ds.Spec.UpdateStrategy.Type, step.wantType)
		}
		if rolling := ds.Spec.UpdateStrategy.RollingUpdate; step.wantMax != "" {
			if rolling == nil || rolling.MaxUnavailable.String() != step.wantMax || rolling.MaxSurge.IntVal != 0 {
				t.Fatalf("step %d: unexpected rolling strategy: %+v", i, rolling)
			}
		} else if rolling != nil {
			t.Fatalf("step %d: OnDelete retained rollingUpdate", i)
		}
		if !reflect.DeepEqual(&ds.Spec.Template, template) {
			t.Fatalf("step %d changed the pod template", i)
		}
		before = ds.ResourceVersion
		if got := patch(); got.ResourceVersion != before {
			t.Fatalf("step %d: repeated reconciliation wrote the DaemonSet", i)
		}
	}
}
