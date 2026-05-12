package clusterpolicy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		"returns true when auto-upgrade is enabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
				},
			},
			want: true,
		},
		"returns true when auto-upgrade is enabled even if sandbox device plugin is enabled": {
			policy: &rblnv1beta1.RBLNClusterPolicy{
				Spec: rblnv1beta1.RBLNClusterPolicySpec{
					Driver: rblnv1beta1.DriverSpec{
						UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{AutoUpgrade: true},
					},
					SandboxDevicePlugin: rblnv1beta1.RBLNSandboxDevicePluginSpec{Enabled: true},
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

func TestReconcileAutoUpgradeAnnotationInPlace(t *testing.T) {
	tests := map[string]struct {
		shouldEnable     bool
		node             *corev1.Node
		wantChanged      bool
		wantAnnotation   string
		wantAnnotationOk bool
	}{
		"adds the auto-upgrade annotation when enabled and node has driver deploy label": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-enabled",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:      labelValueTrue,
						consts.RBLNDeployDriverLabelKey: labelValueTrue,
					},
				},
			},
			wantChanged:      true,
			wantAnnotation:   labelValueTrue,
			wantAnnotationOk: true,
		},
		"removes the auto-upgrade annotation when disabled": {
			shouldEnable: false,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-disabled",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:      labelValueTrue,
						consts.RBLNDeployDriverLabelKey: labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantChanged:      true,
			wantAnnotationOk: false,
		},
		"removes the auto-upgrade annotation for skipped nodes": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-skip",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:      labelValueTrue,
						consts.RBLNDeployDriverLabelKey: labelValueTrue,
						consts.RBLNDeploySkipLabelKey:   labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantChanged:      true,
			wantAnnotationOk: false,
		},
		"does not touch nodes that are not marked as RBLN present": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-non-rbln",
					Labels: map[string]string{
						"kubernetes.io/arch": "amd64",
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantChanged:      false,
			wantAnnotation:   labelValueTrue,
			wantAnnotationOk: true,
		},
		"returns false when annotation is already reconciled": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-already-set",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:      labelValueTrue,
						consts.RBLNDeployDriverLabelKey: labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantChanged:      false,
			wantAnnotation:   labelValueTrue,
			wantAnnotationOk: true,
		},
		"does not set annotation on vm-passthrough node without driver deploy label": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-vmp",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:              labelValueTrue,
						"rebellions.ai/npu.deploy.vfio-manager": labelValueTrue,
					},
				},
			},
			wantChanged:      false,
			wantAnnotationOk: false,
		},
		"removes stale annotation on vm-passthrough node": {
			shouldEnable: true,
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-vmp-stale",
					Labels: map[string]string{
						consts.RBLNPresentLabelKey:              labelValueTrue,
						"rebellions.ai/npu.deploy.vfio-manager": labelValueTrue,
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantChanged:      true,
			wantAnnotationOk: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			changed := reconcileAutoUpgradeAnnotationInPlace(tc.node, tc.shouldEnable)
			if changed != tc.wantChanged {
				t.Fatalf("reconcileAutoUpgradeAnnotationInPlace() changed = %t, want %t", changed, tc.wantChanged)
			}

			got, exists := tc.node.Annotations[driverAutoUpgradeAnnotationKey]
			if exists != tc.wantAnnotationOk {
				t.Fatalf("annotation exists = %t, want %t", exists, tc.wantAnnotationOk)
			}
			if tc.wantAnnotationOk && got != tc.wantAnnotation {
				t.Fatalf("annotation value = %q, want %q", got, tc.wantAnnotation)
			}
		})
	}
}
