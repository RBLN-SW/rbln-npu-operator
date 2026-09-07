package driver

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

func newTestResolverClient(t *testing.T, objs ...client.Object) client.Client {
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

func driverWithSelector(name string, selector map[string]string) rebellionsaiv1alpha1.RBLNDriver {
	return rebellionsaiv1alpha1.RBLNDriver{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       rebellionsaiv1alpha1.RBLNDriverSpec{NodeSelector: selector},
	}
}

func TestValidate(t *testing.T) {
	tests := map[string]struct {
		name     string
		selector map[string]string
		wantErr  bool
	}{
		"empty selector is valid (fallback instance)": {selector: nil, wantErr: false},
		"custom selector is valid":                    {selector: map[string]string{"rebellions.ai/npu.family": "atom"}, wantErr: false},
		"owner key is reserved":                       {selector: map[string]string{"rebellions.ai/npu.driver.owner": "x"}, wantErr: true},
		"deploy key is reserved":                      {selector: map[string]string{"rebellions.ai/npu.deploy.driver": "true"}, wantErr: true},
		"name longer than 63 chars is invalid": {
			name:    strings.Repeat("a", 64),
			wantErr: true,
		},
		"name of exactly 63 chars is valid": {
			name:    strings.Repeat("a", 63),
			wantErr: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			crName := tc.name
			if crName == "" {
				crName = "driver-a"
			}
			instance := &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: crName},
				Spec:       rebellionsaiv1alpha1.RBLNDriverSpec{NodeSelector: tc.selector},
			}
			err := ValidateDriverSpec(instance)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateDriverSpec() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestDesiredOwnerForNode(t *testing.T) {
	base := driverWithSelector("rbln-driver", nil)
	atom := driverWithSelector("rbln-driver-atom", map[string]string{"rebellions.ai/npu.family": "atom"})
	atomCanary := driverWithSelector("rbln-driver-atom-canary", map[string]string{
		"rebellions.ai/npu.family": "atom", "env": "canary",
	})
	rebel := driverWithSelector("rbln-driver-rebel100", map[string]string{"rebellions.ai/npu.family": "rebel100"})
	groupA := driverWithSelector("rbln-driver-a", map[string]string{"group": "a"})
	zoneZ1 := driverWithSelector("rbln-driver-b", map[string]string{"zone": "z1"})

	tests := map[string]struct {
		nodeLabels    map[string]string
		currentOwner  string
		drivers       []rebellionsaiv1alpha1.RBLNDriver
		wantOwner     string
		wantConflicts []string
	}{
		"no drivers -> uncovered": {
			nodeLabels: map[string]string{"rebellions.ai/npu.deploy.driver": "true"},
			drivers:    nil,
			wantOwner:  "",
		},
		"no matching driver -> uncovered": {
			nodeLabels:   map[string]string{"rebellions.ai/npu.deploy.driver": "true"},
			currentOwner: "rbln-driver-atom",
			drivers:      []rebellionsaiv1alpha1.RBLNDriver{atom},
			wantOwner:    "",
		},
		"single match wins": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "atom"},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{atom},
			wantOwner:  "rbln-driver-atom",
		},
		"empty selector is fallback for unmatched node": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "rebel100"},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{base, atom},
			wantOwner:  "rbln-driver",
		},
		"empty selector value does not match absent label": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "atom"},
			drivers: []rebellionsaiv1alpha1.RBLNDriver{
				base, driverWithSelector("rbln-driver-emptyval", map[string]string{"env": ""}),
			},
			wantOwner: "rbln-driver",
		},
		"specific selector beats fallback": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "atom"},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{base, atom},
			wantOwner:  "rbln-driver-atom",
		},
		"specificity chain: canary beats family beats fallback": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "atom", "env": "canary"},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{base, atom, atomCanary},
			wantOwner:  "rbln-driver-atom-canary",
		},
		"disjoint families do not interact": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "rebel100"},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{atom, rebel},
			wantOwner:  "rbln-driver-rebel100",
		},
		"incomparable tie without current owner -> unowned + conflicts": {
			nodeLabels:    map[string]string{"group": "a", "zone": "z1"},
			drivers:       []rebellionsaiv1alpha1.RBLNDriver{zoneZ1, groupA},
			wantOwner:     "",
			wantConflicts: []string{"rbln-driver-a", "rbln-driver-b"},
		},
		"incomparable tie keeps current owner (sticky)": {
			nodeLabels:    map[string]string{"group": "a", "zone": "z1"},
			currentOwner:  "rbln-driver-a",
			drivers:       []rebellionsaiv1alpha1.RBLNDriver{groupA, zoneZ1},
			wantOwner:     "rbln-driver-a",
			wantConflicts: []string{"rbln-driver-a", "rbln-driver-b"},
		},
		"tie of specific drivers keeps matching fallback owner (sticky to base)": {
			nodeLabels:    map[string]string{"group": "a", "zone": "z1"},
			currentOwner:  "rbln-driver",
			drivers:       []rebellionsaiv1alpha1.RBLNDriver{base, groupA, zoneZ1},
			wantOwner:     "rbln-driver",
			wantConflicts: []string{"rbln-driver-a", "rbln-driver-b"},
		},
		"identical selectors tie": {
			nodeLabels: map[string]string{"rebellions.ai/npu.family": "atom"},
			drivers: []rebellionsaiv1alpha1.RBLNDriver{
				atom, driverWithSelector("rbln-driver-atom2", map[string]string{"rebellions.ai/npu.family": "atom"}),
			},
			wantOwner:     "",
			wantConflicts: []string{"rbln-driver-atom", "rbln-driver-atom2"},
		},
		"specificity beats sticky: current base owner loses to new specific driver": {
			nodeLabels:   map[string]string{"rebellions.ai/npu.family": "atom"},
			currentOwner: "rbln-driver",
			drivers:      []rebellionsaiv1alpha1.RBLNDriver{base, atom},
			wantOwner:    "rbln-driver-atom",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			owner, conflicts := desiredOwnerForNode(tc.nodeLabels, tc.currentOwner, tc.drivers)
			if owner != tc.wantOwner {
				t.Errorf("owner = %q, want %q", owner, tc.wantOwner)
			}
			if len(conflicts) != len(tc.wantConflicts) {
				t.Fatalf("conflicts = %v, want %v", conflicts, tc.wantConflicts)
			}
			for i := range conflicts {
				if conflicts[i] != tc.wantConflicts[i] {
					t.Errorf("conflicts = %v, want %v", conflicts, tc.wantConflicts)
				}
			}
		})
	}
}

func TestSelectorStrictSuperset(t *testing.T) {
	tests := map[string]struct {
		a, b map[string]string
		want bool
	}{
		"strict superset":       {map[string]string{"x": "1", "y": "2"}, map[string]string{"x": "1"}, true},
		"superset of empty":     {map[string]string{"x": "1"}, nil, true},
		"equal sets":            {map[string]string{"x": "1"}, map[string]string{"x": "1"}, false},
		"different value":       {map[string]string{"x": "2", "y": "2"}, map[string]string{"x": "1"}, false},
		"smaller set":           {map[string]string{"x": "1"}, map[string]string{"x": "1", "y": "2"}, false},
		"empty vs empty":        {nil, map[string]string{}, false},
		"incomparable same len": {map[string]string{"x": "1", "z": "3"}, map[string]string{"x": "1", "y": "2"}, false},
		"empty value requires key existence": {
			map[string]string{"y": "1", "z": "2"}, map[string]string{"x": ""}, false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := selectorStrictSuperset(tc.a, tc.b); got != tc.want {
				t.Errorf("selectorStrictSuperset(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	ownerKey := "rebellions.ai/npu.driver.owner"
	deployKey := "rebellions.ai/npu.deploy.driver"

	node := func(name string, labels map[string]string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	}

	base := driverWithSelector("rbln-driver", nil)
	atom := driverWithSelector("rbln-driver-atom", map[string]string{"family": "atom"})
	groupA := driverWithSelector("rbln-driver-a", map[string]string{"group": "a"})
	zoneZ1 := driverWithSelector("rbln-driver-b", map[string]string{"zone": "z1"})
	longDriverName := strings.Repeat("a", 64)

	tests := map[string]struct {
		nodes         []*corev1.Node
		drivers       []rebellionsaiv1alpha1.RBLNDriver
		wantOwners    map[string]string // node -> expected owner label ("" = absent)
		wantUncovered []string
		// wantOwnedCount is checked for presence: every entry must exist in
		// OwnedNodes with the given count (a zero must be an explicit series).
		wantOwnedCount map[string]int
		// wantOwnedAbsent asserts these driver names are NOT keys in
		// OwnedNodes at all (excluded from routing, so never zero-seeded).
		wantOwnedAbsent []string
		// wantConflicts, when non-nil, is checked against ConflictNodes with
		// exact equality (map size and each driver's node list).
		wantConflicts map[string][]string
		// wantScopeExits, when non-nil, is checked against ScopeExitNodes
		// with exact equality.
		wantScopeExits []string
		// wantOwnerChangesEmpty asserts OwnerChanges has no entries this pass.
		wantOwnerChangesEmpty bool
	}{
		"fallback and specific split nodes": {
			nodes: []*corev1.Node{
				node("n-atom", map[string]string{deployKey: "true", "family": "atom"}),
				node("n-plain", map[string]string{deployKey: "true"}),
			},
			drivers:        []rebellionsaiv1alpha1.RBLNDriver{base, atom},
			wantOwners:     map[string]string{"n-atom": "rbln-driver-atom", "n-plain": "rbln-driver"},
			wantOwnedCount: map[string]int{"rbln-driver-atom": 1, "rbln-driver": 1},
		},
		"uncovered node loses stale owner label": {
			nodes: []*corev1.Node{
				node("n-orphan", map[string]string{deployKey: "true", ownerKey: "rbln-driver-gone"}),
			},
			drivers:       nil,
			wantOwners:    map[string]string{"n-orphan": ""},
			wantUncovered: []string{"n-orphan"},
		},
		"non-deploy node is out of scope": {
			nodes: []*corev1.Node{
				node("n-pre", map[string]string{deployKey: "pre-installed", "family": "atom"}),
			},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{atom},
			wantOwners: map[string]string{"n-pre": ""},
		},
		"node leaving deploy scope loses owner label": {
			nodes: []*corev1.Node{
				node("n-left", map[string]string{deployKey: "pre-installed", ownerKey: "rbln-driver"}),
			},
			drivers:               []rebellionsaiv1alpha1.RBLNDriver{base},
			wantOwners:            map[string]string{"n-left": ""},
			wantScopeExits:        []string{"n-left"},
			wantOwnerChangesEmpty: true,
		},
		"driver with reserved selector key is excluded from routing": {
			nodes: []*corev1.Node{
				node("n1", map[string]string{deployKey: "true"}),
			},
			drivers: []rebellionsaiv1alpha1.RBLNDriver{
				driverWithSelector("rbln-driver-bad", map[string]string{ownerKey: "x"}),
			},
			wantOwners:    map[string]string{"n1": ""},
			wantUncovered: []string{"n1"},
		},
		"candidate matching nothing reports an explicit zero": {
			nodes: []*corev1.Node{
				node("n-atom", map[string]string{deployKey: "true", "family": "atom"}),
			},
			drivers: []rebellionsaiv1alpha1.RBLNDriver{
				atom,
				driverWithSelector("rbln-driver-nomatch", map[string]string{"family": "none"}),
			},
			wantOwners:     map[string]string{"n-atom": "rbln-driver-atom"},
			wantOwnedCount: map[string]int{"rbln-driver-atom": 1, "rbln-driver-nomatch": 0},
		},
		"sticky owner survives tie and reaches conflict report": {
			nodes: []*corev1.Node{
				node("n-tied", map[string]string{
					deployKey: "true", "group": "a", "zone": "z1", ownerKey: "rbln-driver-a",
				}),
			},
			drivers:    []rebellionsaiv1alpha1.RBLNDriver{groupA, zoneZ1},
			wantOwners: map[string]string{"n-tied": "rbln-driver-a"},
			wantConflicts: map[string][]string{
				"rbln-driver-a": {"n-tied"},
				"rbln-driver-b": {"n-tied"},
			},
			wantOwnerChangesEmpty: true,
		},
		"deleting driver is excluded from routing": {
			nodes: []*corev1.Node{
				node("n1", map[string]string{deployKey: "true"}),
			},
			drivers: []rebellionsaiv1alpha1.RBLNDriver{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "rbln-driver-deleting",
						DeletionTimestamp: &metav1.Time{Time: time.Now()},
						Finalizers:        []string{"test.rebellions.ai/keep"},
					},
				},
			},
			wantOwners:      map[string]string{"n1": ""},
			wantUncovered:   []string{"n1"},
			wantOwnedAbsent: []string{"rbln-driver-deleting"},
		},
		"over-long driver name is excluded": {
			nodes: []*corev1.Node{
				node("n1", map[string]string{deployKey: "true"}),
			},
			drivers:         []rebellionsaiv1alpha1.RBLNDriver{driverWithSelector(longDriverName, nil)},
			wantOwners:      map[string]string{"n1": ""},
			wantUncovered:   []string{"n1"},
			wantOwnedAbsent: []string{longDriverName},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			objs := make([]client.Object, 0, len(tc.nodes)+len(tc.drivers))
			for _, n := range tc.nodes {
				objs = append(objs, n)
			}
			for i := range tc.drivers {
				objs = append(objs, &tc.drivers[i])
			}
			c := newTestResolverClient(t, objs...)
			r := NewOwnerResolver(c)

			result, err := r.Resolve(context.Background())
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			for nodeName, wantOwner := range tc.wantOwners {
				got := &corev1.Node{}
				if err := c.Get(context.Background(), client.ObjectKey{Name: nodeName}, got); err != nil {
					t.Fatalf("get node %s: %v", nodeName, err)
				}
				if got.Labels[ownerKey] != wantOwner {
					t.Errorf("node %s owner = %q, want %q", nodeName, got.Labels[ownerKey], wantOwner)
				}
			}
			if len(result.UncoveredNodes) != len(tc.wantUncovered) {
				t.Errorf("uncovered = %v, want %v", result.UncoveredNodes, tc.wantUncovered)
			}
			for driverName, want := range tc.wantOwnedCount {
				// Presence matters: a zero must be an explicit entry, not a
				// missing map key, so the gauge exposes misconfigured selectors.
				got, ok := result.OwnedNodes[driverName]
				if !ok || len(got) != want {
					t.Errorf("owned[%s] = %d (present=%v), want %d", driverName, len(got), ok, want)
				}
				// The node objects are what pool building consumes, so each
				// one must actually be owned by this driver per wantOwners.
				for _, n := range got {
					if tc.wantOwners[n.Name] != driverName {
						t.Errorf("owned[%s] includes node %s, want owner %q", driverName, n.Name, tc.wantOwners[n.Name])
					}
				}
			}
			for _, driverName := range tc.wantOwnedAbsent {
				if got, ok := result.OwnedNodes[driverName]; ok {
					t.Errorf("owned[%s] = %d, want absent (driver excluded from routing)", driverName, len(got))
				}
			}
			if tc.wantConflicts != nil {
				if len(result.ConflictNodes) != len(tc.wantConflicts) {
					t.Errorf("conflictNodes = %v, want %v", result.ConflictNodes, tc.wantConflicts)
				}
				for driverName, want := range tc.wantConflicts {
					if got := result.ConflictNodes[driverName]; !slices.Equal(got, want) {
						t.Errorf("conflictNodes[%s] = %v, want %v", driverName, got, want)
					}
				}
			}
			if tc.wantScopeExits != nil && !slices.Equal(result.ScopeExitNodes, tc.wantScopeExits) {
				t.Errorf("scopeExits = %v, want %v", result.ScopeExitNodes, tc.wantScopeExits)
			}
			if tc.wantOwnerChangesEmpty && len(result.OwnerChanges) != 0 {
				t.Errorf("ownerChanges = %v, want empty", result.OwnerChanges)
			}
		})
	}
}

func TestResolveIdempotent(t *testing.T) {
	deployKey := "rebellions.ai/npu.deploy.driver"
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{deployKey: "true"}}}
	d := driverWithSelector("rbln-driver", nil)
	c := newTestResolverClient(t, n, &d)
	r := NewOwnerResolver(c)

	first, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if len(first.OwnerChanges) != 1 {
		t.Fatalf("first resolve changes = %v, want 1", first.OwnerChanges)
	}
	second, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if len(second.OwnerChanges) != 0 {
		t.Errorf("second resolve must be a no-op, got changes %v", second.OwnerChanges)
	}
}
