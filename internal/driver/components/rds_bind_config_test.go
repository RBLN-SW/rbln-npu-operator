package components

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestBuildRDSBindConfigData(t *testing.T) {
	tests := map[string]struct {
		selection map[string][]string
		want      map[string]string
	}{
		"nil selection yields empty data": {
			selection: nil,
			want:      map[string]string{},
		},
		"single node single bdf": {
			selection: map[string][]string{"udc-06": {"0000:d8:00.0"}},
			want:      map[string]string{"udc-06": "TARGET_BDFS=\"0000:d8:00.0\"\n"},
		},
		"multiple bdfs are space-joined": {
			selection: map[string][]string{"udc-06": {"0000:d8:00.0", "0000:d9:00.0"}},
			want:      map[string]string{"udc-06": "TARGET_BDFS=\"0000:d8:00.0 0000:d9:00.0\"\n"},
		},
		"node with empty bdfs is skipped": {
			selection: map[string][]string{"udc-06": {}, "udc-07": {"0000:e1:00.0"}},
			want:      map[string]string{"udc-07": "TARGET_BDFS=\"0000:e1:00.0\"\n"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildRDSBindConfigData(tc.selection)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("buildRDSBindConfigData() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func newRDSTestPatcher(t *testing.T, rdsEnabled bool, selection map[string][]string) *driverManagerPatcher {
	t.Helper()
	h := newTestPatcher(t, "")
	h.rdsBindingEnabled = rdsEnabled
	h.rdsDeviceSelection = selection
	return h
}

func TestHandleRDSBindConfigMap_Create(t *testing.T) {
	h := newRDSTestPatcher(t, true, map[string][]string{"udc-06": {"0000:d8:00.0"}})
	owner := newTestOwner()

	if err := h.handleRDSBindConfigMap(context.Background(), owner); err != nil {
		t.Fatalf("handleRDSBindConfigMap() error: %v", err)
	}

	cm := &corev1.ConfigMap{}
	assertObjectExists(t, h.client, types.NamespacedName{
		Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
		Namespace: testNamespace,
	}, cm)

	if got := cm.Data["udc-06"]; got != "TARGET_BDFS=\"0000:d8:00.0\"\n" {
		t.Fatalf("ConfigMap data[udc-06] = %q", got)
	}
	assertHasOwnerRef(t, cm, owner.Name)
}

// The -rds pod mounts this ConfigMap unconditionally, so when RDS is enabled it
// must exist even with no per-node selection (empty data → auto opt-out).
func TestHandleRDSBindConfigMap_EnabledEmptySelectionCreatesEmpty(t *testing.T) {
	h := newRDSTestPatcher(t, true, nil)
	owner := newTestOwner()

	if err := h.handleRDSBindConfigMap(context.Background(), owner); err != nil {
		t.Fatalf("handleRDSBindConfigMap() error: %v", err)
	}

	cm := &corev1.ConfigMap{}
	assertObjectExists(t, h.client, types.NamespacedName{
		Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
		Namespace: testNamespace,
	}, cm)

	if len(cm.Data) != 0 {
		t.Fatalf("expected empty ConfigMap data, got %v", cm.Data)
	}
	assertHasOwnerRef(t, cm, owner.Name)
}

func TestHandleRDSBindConfigMap_DisabledDeletes(t *testing.T) {
	scheme := newTestScheme(t)
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
			Namespace: testNamespace,
		},
	}
	c := newFakeClient(t, scheme, existing)
	h := newTestPatcher(t, "")
	h.client = c
	h.scheme = scheme
	h.rdsBindingEnabled = false

	if err := h.handleRDSBindConfigMap(context.Background(), newTestOwner()); err != nil {
		t.Fatalf("handleRDSBindConfigMap() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
		Namespace: testNamespace,
	}, &corev1.ConfigMap{})
}

func hasVolume(vols []corev1.Volume, name string) bool {
	for i := range vols {
		if vols[i].Name == name {
			return true
		}
	}
	return false
}

func hasInitContainer(cs []corev1.Container, name string) bool {
	for i := range cs {
		if cs[i].Name == name {
			return true
		}
	}
	return false
}

func driverContainerHasMount(cs []corev1.Container, path string) bool {
	for i := range cs {
		if cs[i].Name != driverManagerContainer {
			continue
		}
		for _, m := range cs[i].VolumeMounts {
			if m.MountPath == path && m.SubPath == rdsBindConfFileName {
				return true
			}
		}
	}
	return false
}

func initContainerMountsVolume(cs []corev1.Container, containerName, volName string) bool {
	for i := range cs {
		if cs[i].Name != containerName {
			continue
		}
		for _, m := range cs[i].VolumeMounts {
			if m.Name == volName {
				return true
			}
		}
	}
	return false
}

func TestBuildDriverPodSpec_RDSBindConfigWiring(t *testing.T) {
	tests := map[string]struct {
		poolRDS  bool
		wantWire bool
	}{
		"rds pool gets bind-config wiring":    {poolRDS: true, wantWire: true},
		"base pool has no bind-config wiring": {poolRDS: false, wantWire: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestPatcher(t, "")
			h.rdsBindingEnabled = true
			pool := nodePool{
				osRelease: "ubuntu",
				osVersion: "22.04",
				kernel:    "5.15.0-100-generic",
				rds:       tc.poolRDS,
			}
			if tc.poolRDS {
				pool.nodeSelector = map[string]string{rdsPresentLabelKey: labelValueTrue}
			}

			spec := h.buildDriverPodSpec(pool, "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04")

			if got := hasVolume(spec.Volumes, rdsBindConfigVolumeName); got != tc.wantWire {
				t.Errorf("volume %q present = %v, want %v", rdsBindConfigVolumeName, got, tc.wantWire)
			}
			if got := hasVolume(spec.Volumes, rdsBindConfVolumeName); got != tc.wantWire {
				t.Errorf("volume %q present = %v, want %v", rdsBindConfVolumeName, got, tc.wantWire)
			}
			if got := hasInitContainer(spec.InitContainers, rdsBindConfigSelectInitContainer); got != tc.wantWire {
				t.Errorf("init container %q present = %v, want %v", rdsBindConfigSelectInitContainer, got, tc.wantWire)
			}
			if got := driverContainerHasMount(spec.Containers, rdsBindConfContainerPath); got != tc.wantWire {
				t.Errorf("driver container mount %q present = %v, want %v", rdsBindConfContainerPath, got, tc.wantWire)
			}
			if tc.wantWire {
				// A volume-name typo in the select init container would otherwise
				// only surface at pod-schedule time, so assert both mounts here.
				if !initContainerMountsVolume(spec.InitContainers, rdsBindConfigSelectInitContainer, rdsBindConfigVolumeName) {
					t.Errorf("init container %q missing volume mount %q", rdsBindConfigSelectInitContainer, rdsBindConfigVolumeName)
				}
				if !initContainerMountsVolume(spec.InitContainers, rdsBindConfigSelectInitContainer, rdsBindConfVolumeName) {
					t.Errorf("init container %q missing volume mount %q", rdsBindConfigSelectInitContainer, rdsBindConfVolumeName)
				}
			}
			// The driver-manager init container must stay first so
			// handleDaemonSet's InitContainers[0] digest injection is unaffected.
			if len(spec.InitContainers) == 0 || spec.InitContainers[0].Name != driverManagerInitContainer {
				t.Fatalf("InitContainers[0] = %v, want %q first", spec.InitContainers, driverManagerInitContainer)
			}
		})
	}
}

func TestPatch_RendersRDSBindConfigMap(t *testing.T) {
	scheme := newTestScheme(t)
	owner := newTestOwner()
	c := newFakeClient(t, scheme, owner)

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", nil, true,
		map[string][]string{"udc-06": {"0000:d8:00.0"}})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
		Namespace: testNamespace,
	}, cm)
	if _, ok := cm.Data["udc-06"]; !ok {
		t.Fatalf("bind-config ConfigMap missing key udc-06: %v", cm.Data)
	}

	// Disabling RDS on a later pass must remove the ConfigMap again.
	p, err = NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", nil, false, nil)
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + rdsBindConfigMapSuffix,
		Namespace: testNamespace,
	}, &corev1.ConfigMap{})
}
