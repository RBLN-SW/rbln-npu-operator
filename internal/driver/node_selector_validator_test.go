package driver

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

func newTestValidatorClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		rebellionsaiv1alpha1.AddToScheme,
		corev1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("scheme registration failed: %v", err)
		}
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func TestValidate(t *testing.T) {
	defaultSelector := map[string]string{"rebellions.ai/npu.deploy.driver": "true"}

	tests := map[string]struct {
		nodes   []*corev1.Node
		drivers []*rebellionsaiv1alpha1.RBLNDriver
		target  *rebellionsaiv1alpha1.RBLNDriver
		wantErr bool
	}{
		"no matching nodes passes": {
			nodes: nil,
			target: &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-a"},
			},
			wantErr: false,
		},
		"single driver no conflict": {
			nodes: []*corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: defaultSelector},
			}},
			target: &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-a"},
			},
			wantErr: false,
		},
		"overlapping selectors conflict": {
			nodes: []*corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: defaultSelector},
			}},
			drivers: []*rebellionsaiv1alpha1.RBLNDriver{{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-b"},
			}},
			target: &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-a"},
			},
			wantErr: true,
		},
		"disjoint selectors no conflict": {
			nodes: []*corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: map[string]string{"zone": "a"}},
			}},
			drivers: []*rebellionsaiv1alpha1.RBLNDriver{{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-b"},
				Spec:       rebellionsaiv1alpha1.RBLNDriverSpec{NodeSelector: map[string]string{"zone": "b"}},
			}},
			target: &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-a"},
				Spec:       rebellionsaiv1alpha1.RBLNDriverSpec{NodeSelector: map[string]string{"zone": "a"}},
			},
			wantErr: false,
		},
		"same driver name is skipped": {
			nodes: []*corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: defaultSelector},
			}},
			drivers: nil, // target itself is the only driver
			target: &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "driver-a"},
			},
			wantErr: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var objs []client.Object
			for _, n := range tc.nodes {
				objs = append(objs, n)
			}
			// Pre-create other drivers and the target so List returns them all.
			for _, d := range tc.drivers {
				objs = append(objs, d)
			}
			objs = append(objs, tc.target.DeepCopy())

			c := newTestValidatorClient(t, objs...)
			v := NewNodeSelectorValidator(c)

			err := v.Validate(context.Background(), tc.target)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLabelsMatch(t *testing.T) {
	tests := map[string]struct {
		labels   map[string]string
		selector map[string]string
		want     bool
	}{
		"empty selector matches all": {
			labels:   map[string]string{"a": "1"},
			selector: map[string]string{},
			want:     true,
		},
		"exact match": {
			labels:   map[string]string{"a": "1", "b": "2"},
			selector: map[string]string{"a": "1"},
			want:     true,
		},
		"missing key": {
			labels:   map[string]string{"a": "1"},
			selector: map[string]string{"b": "2"},
			want:     false,
		},
		"wrong value": {
			labels:   map[string]string{"a": "1"},
			selector: map[string]string{"a": "2"},
			want:     false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := labelsMatch(tc.labels, tc.selector)
			if got != tc.want {
				t.Fatalf("labelsMatch() = %v, want %v", got, tc.want)
			}
		})
	}
}
