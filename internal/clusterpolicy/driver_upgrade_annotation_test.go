package clusterpolicy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newAnnotationFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	if err := rblnv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rblnv1beta1 to scheme: %v", err)
	}

	runtimeObjs := make([]client.Object, len(objs))
	copy(runtimeObjs, objs)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runtimeObjs...).
		Build()
}

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
		reason           string
		policy           *rblnv1beta1.RBLNClusterPolicy
		node             *corev1.Node
		wantAnnotation   string
		wantAnnotationOk bool
	}{
		"adds the auto-upgrade annotation when auto-upgrade is enabled": {
			reason: "a present RBLN node with auto-upgrade enabled should receive the annotation",
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
			reason: "a node with the annotation should have it removed when auto-upgrade is turned off",
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
			reason: "nodes with the deploy-skip label should have the annotation removed even when auto-upgrade is enabled",
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
		"does not touch nodes that are not marked as RBLN present": {
			reason: "nodes without the RBLN present label are excluded by the label selector and must not be modified",
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
					Name: "node-non-rbln",
					Labels: map[string]string{
						"kubernetes.io/arch": "amd64",
					},
					Annotations: map[string]string{
						driverAutoUpgradeAnnotationKey: labelValueTrue,
					},
				},
			},
			wantAnnotation:   labelValueTrue,
			wantAnnotationOk: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			k8sClient := newAnnotationFakeClient(t, tc.node)
			nodeList := &corev1.NodeList{Items: []corev1.Node{*tc.node}}

			if err := ReconcileDriverAutoUpgradeAnnotations(context.Background(), k8sClient, tc.policy, nodeList); err != nil {
				t.Fatalf("%s: ReconcileDriverAutoUpgradeAnnotations() unexpected error: %v", tc.reason, err)
			}

			var updated corev1.Node
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: tc.node.Name}, &updated); err != nil {
				t.Fatalf("%s: get node: %v", tc.reason, err)
			}

			got, exists := updated.Annotations[driverAutoUpgradeAnnotationKey]
			if exists != tc.wantAnnotationOk {
				t.Fatalf("%s: annotation exists = %t, want %t", tc.reason, exists, tc.wantAnnotationOk)
			}
			if tc.wantAnnotationOk && got != tc.wantAnnotation {
				t.Fatalf("%s: annotation value = %q, want %q", tc.reason, got, tc.wantAnnotation)
			}
		})
	}
}
