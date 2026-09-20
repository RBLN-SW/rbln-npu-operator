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
)

func TestShouldSkipDaemonSetUpdate(t *testing.T) {
	h := &driverManagerPatcher{
		basePatcher: basePatcher{log: logf.Log},
	}

	// A stamped DaemonSet as handleDaemonSet would hand to the API server:
	// the template hash covers the whole pod spec, not just the driver
	// container, so an init-container-only change (driver-manager tag or
	// env) must still reach the DaemonSet.
	mk := func(mutate func(*corev1.PodSpec)) *appsv1.DaemonSet {
		spec := corev1.PodSpec{
			InitContainers: []corev1.Container{{
				Name:  driverManagerInitContainer,
				Image: "driver-manager:v0.2.2",
				Env:   []corev1.EnvVar{{Name: driverConfigDigestEnv, Value: "digest-1"}},
			}},
			Containers:   []corev1.Container{{Name: driverManagerContainer, Image: "driver:3.0.0"}},
			NodeSelector: map[string]string{driverManagerDeployLabelKey: labelValueTrue},
			Volumes:      []corev1.Volume{{Name: "host-sys"}},
		}
		if mutate != nil {
			mutate(&spec)
		}
		ds := &appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}
		stampTemplateHash(ds)
		return ds
	}

	tests := map[string]struct {
		current  *appsv1.DaemonSet
		desired  *appsv1.DaemonSet
		wantSkip bool
	}{
		"nil current returns false": {
			current:  nil,
			desired:  mk(nil),
			wantSkip: false,
		},
		"current without template hash annotation is updated once": {
			current: func() *appsv1.DaemonSet {
				ds := mk(nil)
				ds.Annotations = nil
				return ds
			}(),
			desired:  mk(nil),
			wantSkip: false,
		},
		// The upgrade controller reads the hash off the pod, so a DaemonSet
		// whose pod template lacks it must be updated even when the
		// DaemonSet-level annotation already matches.
		"current without pod template hash annotation is updated once": {
			current: func() *appsv1.DaemonSet {
				ds := mk(nil)
				ds.Spec.Template.Annotations = nil
				return ds
			}(),
			desired:  mk(nil),
			wantSkip: false,
		},
		"identical template skips update": {
			current:  mk(nil),
			desired:  mk(nil),
			wantSkip: true,
		},
		"init container image change is applied": {
			current:  mk(nil),
			desired:  mk(func(s *corev1.PodSpec) { s.InitContainers[0].Image = "driver-manager:v0.3.0" }),
			wantSkip: false,
		},
		"init container env change is applied": {
			current: mk(nil),
			desired: mk(func(s *corev1.PodSpec) {
				s.InitContainers[0].Env = append(s.InitContainers[0].Env,
					corev1.EnvVar{Name: "NPU_POD_EVICTION_FORCE", Value: "true"})
			}),
			wantSkip: false,
		},
		"driver container change is applied": {
			current:  mk(nil),
			desired:  mk(func(s *corev1.PodSpec) { s.Containers[0].Image = "driver:3.1.0" }),
			wantSkip: false,
		},
		"affinity change is applied": {
			current:  mk(nil),
			desired:  mk(func(s *corev1.PodSpec) { s.Affinity = rdsExclusionAffinity() }),
			wantSkip: false,
		},
		"nodeSelector change is applied": {
			current:  mk(nil),
			desired:  mk(func(s *corev1.PodSpec) { s.NodeSelector[rdsPresentLabelKey] = labelValueTrue }),
			wantSkip: false,
		},
		"volume change is applied": {
			current:  mk(nil),
			desired:  mk(func(s *corev1.PodSpec) { s.Volumes = append(s.Volumes, corev1.Volume{Name: "extra"}) }),
			wantSkip: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := h.shouldSkipDaemonSetUpdate(tc.current, tc.desired)
			if got != tc.wantSkip {
				t.Fatalf("shouldSkipDaemonSetUpdate() = %v, want %v", got, tc.wantSkip)
			}
		})
	}
}

// The template hash gates every DaemonSet update, so a non-deterministic
// field in the rendered pod spec would re-stamp a new hash each reconcile
// and, with autoUpgrade on, roll the fleet every pass.
func TestStampTemplateHashIsDeterministic(t *testing.T) {
	h := newTestPatcher(t, "")
	pool := nodePool{osRelease: "ubuntu", osVersion: "22.04", kernel: "5.15.0-100-generic", family: "atom"}
	const image = "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04"

	stamp := func() string {
		ds := &appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
			Spec: *h.buildDriverPodSpec(pool, image),
		}}}
		return stampTemplateHash(ds)
	}
	// Go randomizes map iteration per range statement, so two renders agree
	// by chance about half the time on a small map; many renders make an
	// unsorted iteration in the render path fail reliably.
	first := stamp()
	for i := 0; i < 32; i++ {
		if again := stamp(); again != first {
			t.Fatalf("template hash differs across identical renders: %s vs %s", first, again)
		}
	}
}

// The hash is stamped on the pod template as well as on the DaemonSet, so the
// upgrade controller can read it off a running pod. The stamp lives in
// template metadata, outside the hashed pod spec, so stamping must not change
// the hash.
func TestStampTemplateHashStampsPodTemplate(t *testing.T) {
	ds := &appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: driverManagerContainer, Image: "driver:3.0.0"}}},
	}}}

	hash := stampTemplateHash(ds)
	if hash == "" {
		t.Fatal("stampTemplateHash returned an empty hash")
	}
	if got := ds.Annotations[driverLastAppliedTemplateHashAnnotation]; got != hash {
		t.Fatalf("DaemonSet annotation = %q, want %q", got, hash)
	}
	if got := ds.Spec.Template.Annotations[driverLastAppliedTemplateHashAnnotation]; got != hash {
		t.Fatalf("pod template annotation = %q, want %q", got, hash)
	}
	if again := stampTemplateHash(ds); again != hash {
		t.Fatalf("re-stamping changed the hash: %s vs %s", hash, again)
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
