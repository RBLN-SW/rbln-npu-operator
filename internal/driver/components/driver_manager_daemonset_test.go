package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func TestShouldSkipDaemonSetUpdateByDriverHash(t *testing.T) {
	h := &driverManagerPatcher{
		basePatcher: basePatcher{log: logf.Log},
	}

	tests := map[string]struct {
		current  *appsv1.DaemonSet
		digest   string
		wantSkip bool
	}{
		"matching annotation hash skips update": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{driverLastAppliedHashAnnotation: "abc123"},
				},
			},
			digest:   "abc123",
			wantSkip: true,
		},
		"different annotation hash allows update": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{driverLastAppliedHashAnnotation: "old-hash"},
				},
			},
			digest:   "new-hash",
			wantSkip: false,
		},
		"missing annotation falls back to container hash": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
				Spec: appsv1.DaemonSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "test", Image: "img:v1"}},
						},
					},
				},
			},
			digest:   k8sutil.GetObjectHash([]corev1.Container{{Name: "test", Image: "img:v1"}}),
			wantSkip: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := h.shouldSkipDaemonSetUpdateByDriverHash(tc.current, tc.digest)
			if got != tc.wantSkip {
				t.Fatalf("shouldSkipDaemonSetUpdateByDriverHash() = %v, want %v", got, tc.wantSkip)
			}
		})
	}
}

func TestUpsertEnvVar(t *testing.T) {
	tests := map[string]struct {
		base   []corev1.EnvVar
		target corev1.EnvVar
		want   string
	}{
		"insert new": {
			base:   []corev1.EnvVar{{Name: "A", Value: "1"}},
			target: corev1.EnvVar{Name: "B", Value: "2"},
			want:   "2",
		},
		"update existing": {
			base:   []corev1.EnvVar{{Name: "A", Value: "old"}},
			target: corev1.EnvVar{Name: "A", Value: "new"},
			want:   "new",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := upsertEnvVar(tc.base, tc.target)
			found := false
			for _, env := range result {
				if env.Name == tc.target.Name {
					if env.Value != tc.want {
						t.Fatalf("env %q = %q, want %q", tc.target.Name, env.Value, tc.want)
					}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("env %q not found in result", tc.target.Name)
			}
		})
	}
}

func TestDriverManagerLabels(t *testing.T) {
	h := &driverManagerPatcher{
		basePatcher: basePatcher{name: driverManagerName, instanceName: testInstanceName},
	}
	pool := nodePool{name: "ubuntu22.04-5.15.0"}

	labels := h.driverManagerLabels(pool)

	if labels[driverManagerAppLabelKey] != driverManagerName {
		t.Fatalf("app label = %q, want %q", labels[driverManagerAppLabelKey], driverManagerName)
	}
	if labels[driverManagerNodePoolLabelKey] != pool.name {
		t.Fatalf("pool label = %q, want %q", labels[driverManagerNodePoolLabelKey], pool.name)
	}
	if labels[driverManagerInstanceLabelKey] != testInstanceName {
		t.Fatalf("instance label = %q, want %q", labels[driverManagerInstanceLabelKey], testInstanceName)
	}
}

func newStaleTestDaemonSet(name, instanceName, poolName string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels: map[string]string{
				driverManagerAppLabelKey:      driverManagerName,
				driverManagerNodePoolLabelKey: poolName,
				driverManagerInstanceLabelKey: instanceName,
			},
		},
	}
}

func TestCleanUpStaleDaemonSets(t *testing.T) {
	tests := map[string]struct {
		existingDS       []*appsv1.DaemonSet
		desiredPoolNames []string
		wantDeleted      []string // DS names that should be deleted
		wantKept         []string // DS names that should remain
	}{
		"deletes orphaned DaemonSets": {
			existingDS: []*appsv1.DaemonSet{
				newStaleTestDaemonSet(testInstanceName+"-ubuntu22.04-5.15.0", testInstanceName, "ubuntu22.04-5.15.0"),
				newStaleTestDaemonSet(testInstanceName+"-ubuntu22.04-5.19.0", testInstanceName, "ubuntu22.04-5.19.0"),
			},
			desiredPoolNames: []string{"ubuntu22.04-5.19.0"},
			wantDeleted:      []string{testInstanceName + "-ubuntu22.04-5.15.0"},
			wantKept:         []string{testInstanceName + "-ubuntu22.04-5.19.0"},
		},
		"no stale DaemonSets": {
			existingDS: []*appsv1.DaemonSet{
				newStaleTestDaemonSet(testInstanceName+"-ubuntu22.04-5.15.0", testInstanceName, "ubuntu22.04-5.15.0"),
			},
			desiredPoolNames: []string{"ubuntu22.04-5.15.0"},
			wantDeleted:      nil,
			wantKept:         []string{testInstanceName + "-ubuntu22.04-5.15.0"},
		},
		"all DaemonSets stale": {
			existingDS: []*appsv1.DaemonSet{
				newStaleTestDaemonSet(testInstanceName+"-ubuntu22.04-5.15.0", testInstanceName, "ubuntu22.04-5.15.0"),
				newStaleTestDaemonSet(testInstanceName+"-rhel9-5.14.0", testInstanceName, "rhel9-5.14.0"),
			},
			desiredPoolNames: []string{"ubuntu22.04-6.0.0"},
			wantDeleted:      []string{testInstanceName + "-ubuntu22.04-5.15.0", testInstanceName + "-rhel9-5.14.0"},
			wantKept:         nil,
		},
		"ignores other instances": {
			existingDS: []*appsv1.DaemonSet{
				newStaleTestDaemonSet(testInstanceName+"-ubuntu22.04-5.15.0", testInstanceName, "ubuntu22.04-5.15.0"),
				newStaleTestDaemonSet("other-instance-ubuntu22.04-5.15.0", "other-instance", "ubuntu22.04-5.15.0"),
			},
			desiredPoolNames: []string{"ubuntu22.04-5.19.0"},
			wantDeleted:      []string{testInstanceName + "-ubuntu22.04-5.15.0"},
			wantKept:         []string{"other-instance-ubuntu22.04-5.15.0"},
		},
		"empty existing list": {
			existingDS:       nil,
			desiredPoolNames: []string{"ubuntu22.04-5.15.0"},
			wantDeleted:      nil,
			wantKept:         nil,
		},
		"empty pool name never matches a malformed DaemonSet": {
			// A DaemonSet with no node-pool label at all (poolName == "")
			// must never be immortalized by an equally-empty entry in
			// desiredPoolNames; there is no such entry here, so this must
			// still be reaped like any other stale DaemonSet.
			existingDS: []*appsv1.DaemonSet{
				newStaleTestDaemonSet(testInstanceName+"-no-pool-label", testInstanceName, ""),
			},
			desiredPoolNames: []string{"ubuntu22.04-5.15.0"},
			wantDeleted:      []string{testInstanceName + "-no-pool-label"},
			wantKept:         nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := newFakeClientWithDS(t, scheme, tc.existingDS)

			h := &driverManagerPatcher{
				basePatcher: basePatcher{
					client:       c,
					log:          logf.Log,
					name:         driverManagerName,
					instanceName: testInstanceName,
					namespace:    testNamespace,
				},
			}

			desiredPoolNames := make(map[string]struct{}, len(tc.desiredPoolNames))
			for _, name := range tc.desiredPoolNames {
				desiredPoolNames[name] = struct{}{}
			}

			ctx := context.Background()
			stale, err := h.findStaleDaemonSets(ctx, desiredPoolNames)
			if err != nil {
				t.Fatalf("findStaleDaemonSets() error: %v", err)
			}
			if err := h.reapDaemonSets(ctx, stale); err != nil {
				t.Fatalf("reapDaemonSets() error: %v", err)
			}

			for _, dsName := range tc.wantDeleted {
				assertObjectNotExists(t, c, types.NamespacedName{Name: dsName, Namespace: testNamespace}, &appsv1.DaemonSet{})
			}
			for _, dsName := range tc.wantKept {
				assertObjectExists(t, c, types.NamespacedName{Name: dsName, Namespace: testNamespace}, &appsv1.DaemonSet{})
			}
		})
	}
}

func newFakeClientWithDS(t *testing.T, scheme *runtime.Scheme, dsList []*appsv1.DaemonSet) client.Client {
	t.Helper()
	objs := make([]client.Object, 0, len(dsList))
	for _, ds := range dsList {
		objs = append(objs, ds)
	}
	return newFakeClient(t, scheme, objs...)
}
