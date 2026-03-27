package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestIsReady(t *testing.T) {
	const (
		name      = "test-component"
		namespace = "test-ns"
	)
	ctx := context.Background()

	tests := map[string]struct {
		ds      *appsv1.DaemonSet
		wantErr bool
	}{
		"DaemonSet not found": {
			ds:      nil,
			wantErr: true,
		},
		"DesiredNumberScheduled is zero": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 0,
					NumberReady:            0,
				},
			},
			wantErr: true,
		},
		"NumberReady < DesiredNumberScheduled": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 3,
					NumberReady:            1,
					NumberUnavailable:      2,
				},
			},
			wantErr: true,
		},
		"all pods ready": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 3,
					NumberReady:            3,
					NumberUnavailable:      0,
				},
			},
			wantErr: false,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			scheme := newTestScheme(t)
			var c client.Client
			if tc.ds != nil {
				c = newFakeClient(t, scheme, tc.ds)
			} else {
				c = newFakeClient(t, scheme)
			}

			bp := &basePatcher{
				client:    c,
				name:      name,
				namespace: namespace,
			}

			err := bp.IsReady(ctx)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
