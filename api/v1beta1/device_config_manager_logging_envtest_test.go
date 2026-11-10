package v1beta1_test

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
)

func TestDeviceConfigManagerLoggingAdmission(t *testing.T) {
	requireEnvtest(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name, level, format, errorField string
	}{
		{name: "configured", level: "debug", format: "text"},
		{name: "warn-alias", level: "warn", format: "json"},
		{name: "partial", level: "info"},
		{name: "invalid-level", level: "trace", errorField: "logging.level"},
		{name: "invalid-format", format: "yaml", errorField: "logging.format"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp := &rblnv1beta1.RBLNClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "dcm-logging-" + tc.name},
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					WorkloadType: "container",
					DeviceConfigManager: rblnv1beta1.RBLNDeviceConfigManagerSpec{
						Enabled: true,
						Logging: &rblnv1beta1.LoggingSpec{Level: tc.level, Format: tc.format},
					},
				},
			}
			err := testK8s.Create(ctx, cp)
			if tc.errorField != "" {
				if err == nil {
					t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })
					t.Fatal("invalid logging configuration was accepted")
				}
				if !strings.Contains(err.Error(), tc.errorField) {
					t.Fatalf("unexpected validation failure: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = testK8s.Delete(ctx, cp) })
			got := &rblnv1beta1.RBLNClusterPolicy{}
			if err := testK8s.Get(ctx, client.ObjectKeyFromObject(cp), got); err != nil {
				t.Fatal(err)
			}
			logging := got.Spec.DeviceConfigManager.Logging
			if logging == nil || logging.Level != tc.level || logging.Format != tc.format {
				t.Fatalf("logging configuration was pruned or changed: %+v", logging)
			}
		})
	}
}
