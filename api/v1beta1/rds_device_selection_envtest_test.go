package v1beta1_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func newRDSTestCR(name string, rds map[string]any) *unstructured.Unstructured {
	emptyNested := map[string]any{}
	cp := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "rebellions.ai/v1beta1",
			"kind":       "RBLNClusterPolicy",
			"metadata":   map[string]any{"name": name},
			"spec": map[string]any{
				"vfioManager":         emptyNested,
				"sandboxDevicePlugin": emptyNested,
				"devicePlugin":        emptyNested,
				"metricsExporter":     emptyNested,
				"rblnDaemon":          emptyNested,
				"npuFeatureDiscovery": emptyNested,
				"containerToolkit":    emptyNested,
				"driver":              emptyNested,
				"rds":                 rds,
			},
		},
	}
	cp.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "rebellions.ai", Version: "v1beta1", Kind: "RBLNClusterPolicy",
	})
	return cp
}

func TestRBLNClusterPolicy_RDSDeviceSelectionRejected(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	cases := map[string]struct {
		rds       map[string]any
		errSubstr string
	}{
		"invalid BDF format": {
			rds: map[string]any{
				"enabled": true,
				"deviceSelection": []any{
					map[string]any{"nodeName": "udc-06", "targetBDFs": []any{"not-a-bdf"}},
				},
			},
			errSubstr: "should match",
		},
		"empty targetBDFs": {
			rds: map[string]any{
				"enabled": true,
				"deviceSelection": []any{
					map[string]any{"nodeName": "udc-06", "targetBDFs": []any{}},
				},
			},
			errSubstr: "should have at least 1 items",
		},
		"empty nodeName": {
			rds: map[string]any{
				"enabled": true,
				"deviceSelection": []any{
					map[string]any{"nodeName": "", "targetBDFs": []any{"0000:d8:00.0"}},
				},
			},
			errSubstr: "should be at least 1 chars long",
		},
		"duplicate nodeName": {
			rds: map[string]any{
				"enabled": true,
				"deviceSelection": []any{
					map[string]any{"nodeName": "udc-06", "targetBDFs": []any{"0000:d8:00.0"}},
					map[string]any{"nodeName": "udc-06", "targetBDFs": []any{"0000:d9:00.0"}},
				},
			},
			errSubstr: "Duplicate value",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cp := newRDSTestCR("rds-rej-"+strings.ToLower(strings.ReplaceAll(name, " ", "-")), tc.rds)
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

func TestRBLNClusterPolicy_RDSDeviceSelectionAccepted(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()

	cp := newRDSTestCR("rds-ok", map[string]any{
		"enabled": true,
		"deviceSelection": []any{
			map[string]any{"nodeName": "udc-06", "targetBDFs": []any{"0000:d8:00.0", "0000:d9:00.0"}},
		},
	})
	if err := testK8s.Create(ctx, cp); err != nil {
		t.Fatalf("valid deviceSelection should be accepted, got: %v", err)
	}
	t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })

	got := &rblnv1beta1.RBLNClusterPolicy{}
	if err := testK8s.Get(ctx, client.ObjectKey{Name: "rds-ok"}, got); err != nil {
		t.Fatalf("get CR back: %v", err)
	}

	if len(got.Spec.RDS.DeviceSelection) != 1 {
		t.Fatalf("DeviceSelection length = %d, want 1", len(got.Spec.RDS.DeviceSelection))
	}
	sel := got.Spec.RDS.DeviceSelection[0]
	if sel.NodeName != "udc-06" {
		t.Errorf("NodeName = %q, want %q", sel.NodeName, "udc-06")
	}
	gotBDFs := make([]string, 0, len(sel.TargetBDFs))
	for _, b := range sel.TargetBDFs {
		gotBDFs = append(gotBDFs, string(b))
	}
	wantBDFs := []string{"0000:d8:00.0", "0000:d9:00.0"}
	if !slices.Equal(gotBDFs, wantBDFs) {
		t.Errorf("TargetBDFs = %v, want %v", gotBDFs, wantBDFs)
	}
}
