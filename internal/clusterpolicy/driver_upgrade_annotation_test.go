package clusterpolicy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestShouldEnableDriverAutoUpgrade(t *testing.T) {
	tests := map[string]struct {
		policy *rblnv1beta1.RBLNClusterPolicy
		want   bool
	}{
		"returns false when upgrade policy is missing": {
			policy: &rblnv1beta1.RBLNClusterPolicy{},
			want:   false,
		},
		"returns true when auto-upgrade is enabled and sandbox device plugin is disabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
				},
			},
			want: true,
		},
		"returns false when auto-upgrade is disabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: false},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
				},
			},
			want: false,
		},
		"returns false when sandbox device plugin is enabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: true},
				},
			},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := shouldEnableDriverAutoUpgrade(tc.policy); got != tc.want {
				t.Fatalf("shouldEnableDriverAutoUpgrade() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestReconcileDriverAutoUpgradeAnnotations(t *testing.T) {
	tests := map[string]struct {
		policy           *rblnv1beta1.RBLNClusterPolicy
		node             *corev1.Node
		wantAnnotation   string
		wantAnnotationOk bool
	}{
		"adds the auto-upgrade annotation when auto-upgrade is enabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
				},
			},
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-enabled",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey: labelValueTrue,
					},
				},
			},
			wantAnnotation:   labelValueTrue,
			wantAnnotationOk: true,
		},
		"removes the auto-upgrade annotation when auto-upgrade is disabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{},
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-disabled",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey: labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantAnnotationOk: false,
		},
		"removes the auto-upgrade annotation for skipped nodes": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: false},
				},
			},
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-skip",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:    labelValueTrue,
						consts.RBLNDeploySkipLabelKey: labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantAnnotationOk: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			k8sClient := newFakeClient(t, tc.node)

			if err := ReconcileDriverAutoUpgradeAnnotations(context.Background(), k8sClient, tc.policy); err != nil {
				t.Fatalf("ReconcileDriverAutoUpgradeAnnotations() unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: tc.node.Name}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}

			got, exists := updated.Annotations[driverAutoUpgradeAnnotationKey]
			if exists != tc.wantAnnotationOk {
				t.Fatalf("annotation exists = %t, want %t", exists, tc.wantAnnotationOk)
			}
			if tc.wantAnnotationOk && got != tc.wantAnnotation {
				t.Fatalf("annotation value = %q, want %q", got, tc.wantAnnotation)
			}
		})
	}
}
