package clusterpolicy

import (
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	if err := rblnv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rblnv1beta1 to scheme: %v", err)
	}

	return scheme
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	runtimeObjs := make([]client.Object, len(objs))
	copy(runtimeObjs, objs)

	return fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithObjects(runtimeObjs...).
		Build()
}

func newTestClusterPolicyService(k8sClient client.Client, workloadType string) *ClusterPolicyService {
	return &ClusterPolicyService{
		client: k8sClient,
		log:    logr.Discard(),
		policy: &rblnv1beta1.RBLNClusterPolicy{
			Spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType: workloadType,
			},
		},
	}
}

func newObjectMeta(name string, labelsMap map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:   name,
		Labels: labelsMap,
	}
}
