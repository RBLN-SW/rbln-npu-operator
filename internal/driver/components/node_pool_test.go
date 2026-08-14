package components

import (
	"reflect"
	"strings"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// TestBuildNodePoolSplitsByFamily pins the family axis: two nodes that are
// otherwise identical (same os/kernel/owner) but carry different family
// values must never collapse into one pool, since each family's driver
// image lives at a different pull path.
func TestBuildNodePoolSplitsByFamily(t *testing.T) {
	atomNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "n-atom",
		Labels: map[string]string{
			driverManagerDeployLabelKey:    "true",
			consts.RBLNDriverOwnerLabelKey: "cr-a",
			consts.RBLNNPUFamilyLabelKey:   "atom",
			consts.NFDOSReleaseIDLabelKey:  "ubuntu",
			consts.NFDOSVersionIDLabelKey:  "22.04",
			consts.NFDKernelLabelKey:       "5.15.0-100-generic",
		},
	}}
	rebel100Node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "n-rebel100",
		Labels: map[string]string{
			driverManagerDeployLabelKey:    "true",
			consts.RBLNDriverOwnerLabelKey: "cr-a",
			consts.RBLNNPUFamilyLabelKey:   "rebel100",
			consts.NFDOSReleaseIDLabelKey:  "ubuntu",
			consts.NFDOSVersionIDLabelKey:  "22.04",
			consts.NFDKernelLabelKey:       "5.15.0-100-generic",
		},
	}}
	pools, nodesWithoutFamily := buildNodePools([]corev1.Node{*atomNode, *rebel100Node}, map[string]string{
		consts.RBLNDriverOwnerLabelKey: "cr-a",
	}, logf.Log)
	if len(nodesWithoutFamily) != 0 {
		t.Errorf("nodesWithoutFamily = %v, want none", nodesWithoutFamily)
	}
	if len(pools) != 2 {
		t.Fatalf("pools = %d, want exactly 2 (one per family): %+v", len(pools), pools)
	}

	poolsByName := make(map[string]nodePool, len(pools))
	for _, p := range pools {
		poolsByName[p.name] = p
	}

	atomPool, ok := poolsByName["atom-ubuntu22.04-5.15.0-100-generic"]
	if !ok {
		t.Fatalf("missing atom pool, got pools: %+v", pools)
	}
	if got := atomPool.nodeSelector[consts.RBLNNPUFamilyLabelKey]; got != "atom" {
		t.Errorf("atom pool nodeSelector family = %q, want %q", got, "atom")
	}

	rebel100Pool, ok := poolsByName["rebel100-ubuntu22.04-5.15.0-100-generic"]
	if !ok {
		t.Fatalf("missing rebel100 pool, got pools: %+v", pools)
	}
	if got := rebel100Pool.nodeSelector[consts.RBLNNPUFamilyLabelKey]; got != "rebel100" {
		t.Errorf("rebel100 pool nodeSelector family = %q, want %q", got, "rebel100")
	}
}

// A node missing the family label produces no pool, but must still be named
// in the skipped list: a silent drop leaves it uncovered with no signal.
func TestBuildNodePoolsSkipsAndReportsNodesWithoutFamily(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "n-no-family",
		Labels: map[string]string{
			driverManagerDeployLabelKey:    "true",
			consts.RBLNDriverOwnerLabelKey: "cr-a",
			consts.NFDOSReleaseIDLabelKey:  "ubuntu",
			consts.NFDOSVersionIDLabelKey:  "22.04",
			consts.NFDKernelLabelKey:       "5.15.0-100-generic",
		},
	}}
	pools, nodesWithoutFamily := buildNodePools([]corev1.Node{*node}, map[string]string{
		consts.RBLNDriverOwnerLabelKey: "cr-a",
	}, logf.Log)
	if len(pools) != 0 {
		t.Fatalf("pools = %d, want 0 (no family label to compose an image path with): %+v", len(pools), pools)
	}
	if len(nodesWithoutFamily) != 1 || nodesWithoutFamily[0] != "n-no-family" {
		t.Fatalf("nodesWithoutFamily = %v, want [n-no-family]", nodesWithoutFamily)
	}
}

// TestBuildNodePoolsMixedFamilyAndNoFamily pins the mixed-fleet contract: one
// call can produce both valid pools and skipped nodes at once — a family
// label gap on some nodes must not suppress pool discovery for the others.
func TestBuildNodePoolsMixedFamilyAndNoFamily(t *testing.T) {
	labeled := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "n-labeled",
		Labels: map[string]string{
			driverManagerDeployLabelKey:    "true",
			consts.RBLNDriverOwnerLabelKey: "cr-a",
			consts.RBLNNPUFamilyLabelKey:   "atom",
			consts.NFDOSReleaseIDLabelKey:  "ubuntu",
			consts.NFDOSVersionIDLabelKey:  "22.04",
			consts.NFDKernelLabelKey:       "5.15.0-100-generic",
		},
	}}
	unlabeled := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "n-unlabeled",
		Labels: map[string]string{
			driverManagerDeployLabelKey:    "true",
			consts.RBLNDriverOwnerLabelKey: "cr-a",
			consts.NFDOSReleaseIDLabelKey:  "ubuntu",
			consts.NFDOSVersionIDLabelKey:  "22.04",
			consts.NFDKernelLabelKey:       "6.8.0-45-generic",
		},
	}}
	pools, nodesWithoutFamily := buildNodePools([]corev1.Node{*labeled, *unlabeled}, map[string]string{
		consts.RBLNDriverOwnerLabelKey: "cr-a",
	}, logf.Log)
	if len(pools) == 0 {
		t.Fatalf("pools = %d, want > 0 (the labeled node should still produce a pool)", len(pools))
	}
	if len(nodesWithoutFamily) == 0 {
		t.Fatalf("nodesWithoutFamily = %d, want > 0 (the unlabeled node must be reported)", len(nodesWithoutFamily))
	}
	if len(pools) != 1 || pools[0].name != "atom-ubuntu22.04-5.15.0-100-generic" {
		t.Fatalf("pools = %+v, want exactly the labeled node's pool", pools)
	}
	if len(nodesWithoutFamily) != 1 || nodesWithoutFamily[0] != "n-unlabeled" {
		t.Fatalf("nodesWithoutFamily = %v, want [n-unlabeled]", nodesWithoutFamily)
	}
}

// TestBuildNodePoolsSortsNodesWithoutFamily pins the sort order of the
// skipped-node report: a caller that surfaces this list (e.g. in a
// condition message) needs a stable, deterministic order, not the caller's
// node order, so the input is deliberately non-alphabetical.
func TestBuildNodePoolsSortsNodesWithoutFamily(t *testing.T) {
	nodes := make([]corev1.Node, 0, 3)
	for _, name := range []string{"n-zeta", "n-alpha", "n-mid"} {
		nodes = append(nodes, corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				driverManagerDeployLabelKey:    "true",
				consts.RBLNDriverOwnerLabelKey: "cr-a",
				consts.NFDOSReleaseIDLabelKey:  "ubuntu",
				consts.NFDOSVersionIDLabelKey:  "22.04",
				consts.NFDKernelLabelKey:       "5.15.0-100-generic",
			},
		}})
	}

	pools, nodesWithoutFamily := buildNodePools(nodes, map[string]string{
		consts.RBLNDriverOwnerLabelKey: "cr-a",
	}, logf.Log)
	if len(pools) != 0 {
		t.Fatalf("pools = %d, want 0 (none of these nodes carry the family label): %+v", len(pools), pools)
	}

	want := []string{"n-alpha", "n-mid", "n-zeta"}
	if !reflect.DeepEqual(nodesWithoutFamily, want) {
		t.Fatalf("nodesWithoutFamily = %v, want %v (sorted ascending)", nodesWithoutFamily, want)
	}
}

func TestGetSanitizedKernelVersion(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"strips x86_64": {
			input: "5.14.0-427.13.1.el8_9.x86_64",
			want:  "5.14.0-427.13.1.el8.9",
		},
		"strips aarch64": {
			input: "5.15.0-100-generic.aarch64",
			want:  "5.15.0-100-generic",
		},
		"no arch suffix unchanged": {
			input: "5.15.0-100-generic",
			want:  "5.15.0-100-generic",
		},
		"underscores to dots": {
			input: "5.14.0-427_el9",
			want:  "5.14.0-427.el9",
		},
		"lowercased": {
			input: "5.15.0-ABC",
			want:  "5.15.0-abc",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := getSanitizedKernelVersion(tc.input)
			if got != tc.want {
				t.Fatalf("getSanitizedKernelVersion(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestBuildNodeSelectorKeepsDeployGateAndOwner(t *testing.T) {
	got := buildNodeSelector(map[string]string{consts.RBLNDriverOwnerLabelKey: "rbln-driver-atom"})
	if got[driverManagerDeployLabelKey] != "true" {
		t.Errorf("deploy gate missing: %v", got)
	}
	if got[consts.RBLNDriverOwnerLabelKey] != "rbln-driver-atom" {
		t.Errorf("owner selector missing: %v", got)
	}
	if len(got) != 2 {
		t.Errorf("unexpected extra selector keys: %v", got)
	}
}

func TestBuildNodePool_MissingLabels(t *testing.T) {
	logger := logf.Log

	tests := map[string]struct {
		labels map[string]string
	}{
		"missing OS release ID": {
			labels: map[string]string{
				consts.NFDOSVersionIDLabelKey: "22.04",
				consts.NFDKernelLabelKey:      "5.15.0",
			},
		},
		"missing OS version ID": {
			labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDKernelLabelKey:      "5.15.0",
			},
		},
		"missing kernel version": {
			labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDOSVersionIDLabelKey: "22.04",
			},
		},
		"no labels at all": {
			labels: map[string]string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "test-node", Labels: tc.labels},
			}
			_, ok := buildNodePool(node, map[string]string{}, logger)
			if ok {
				t.Fatal("expected buildNodePool to return false for missing labels")
			}
		})
	}
}

func TestBuildNodePool_AllLabels(t *testing.T) {
	logger := logf.Log
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDOSVersionIDLabelKey: "22.04",
				consts.NFDKernelLabelKey:      "5.15.0-100-generic",
			},
		},
	}

	pool, ok := buildNodePool(node, map[string]string{}, logger)
	if !ok {
		t.Fatal("expected buildNodePool to return true")
	}
	if pool.osRelease != "ubuntu" {
		t.Fatalf("osRelease = %q, want ubuntu", pool.osRelease)
	}
	if pool.osVersion != "22.04" {
		t.Fatalf("osVersion = %q, want 22.04", pool.osVersion)
	}
	if pool.kernel != "5.15.0-100-generic" {
		t.Fatalf("kernel = %q, want 5.15.0-100-generic", pool.kernel)
	}
	if pool.getOS() != "ubuntu22.04" {
		t.Fatalf("getOS() = %q, want ubuntu22.04", pool.getOS())
	}
}

// A family value that is a legal Kubernetes label but not a DNS-1123 label
// must be rejected, never rewritten: it is spliced verbatim into DaemonSet
// names and image paths.
func TestBuildNodePool_FamilyLabelValidation(t *testing.T) {
	logger := logf.Log

	baseLabels := func(family string) map[string]string {
		labels := map[string]string{
			consts.NFDOSReleaseIDLabelKey: "ubuntu",
			consts.NFDOSVersionIDLabelKey: "22.04",
			consts.NFDKernelLabelKey:      "5.15.0-100-generic",
		}
		if family != "" {
			labels[consts.RBLNNPUFamilyLabelKey] = family
		}
		return labels
	}

	tests := map[string]struct {
		rawFamily  string
		wantFamily string
	}{
		"lowercase alnum accepted": {
			rawFamily:  "atom",
			wantFamily: "atom",
		},
		"hyphenated accepted": {
			rawFamily:  "rebel-100",
			wantFamily: "rebel-100",
		},
		"surrounding whitespace trimmed": {
			rawFamily:  "  atom  ",
			wantFamily: "atom",
		},
		"uppercase rejected": {
			rawFamily:  "Atom",
			wantFamily: "",
		},
		"underscore rejected": {
			rawFamily:  "rebel_100",
			wantFamily: "",
		},
		"mixed case with hyphen rejected": {
			rawFamily:  "ATOM-X",
			wantFamily: "",
		},
		"whitespace-only rejected": {
			rawFamily:  "   ",
			wantFamily: "",
		},
		"leading hyphen rejected": {
			rawFamily:  "-atom",
			wantFamily: "",
		},
		"over 63 chars rejected": {
			rawFamily:  strings.Repeat("a", 64),
			wantFamily: "",
		},
		"long but valid family exceeds composed pool-name label limit": {
			// 40 lowercase letters is a perfectly valid DNS-1123 label on its
			// own (<=63 chars), but "<40 a's>-ubuntu22.04-5.15.0-100-generic"
			// is 71 chars -- over the 63-char label-VALUE cap the composed
			// name must respect once it becomes the node-pool label's value.
			rawFamily:  strings.Repeat("a", 40),
			wantFamily: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "test-node", Labels: baseLabels(tc.rawFamily)},
			}
			pool, ok := buildNodePool(node, map[string]string{}, logger)
			if !ok {
				t.Fatal("expected buildNodePool to return true (os/kernel labels are present)")
			}
			if pool.family != tc.wantFamily {
				t.Fatalf("pool.family = %q, want %q", pool.family, tc.wantFamily)
			}
			if tc.wantFamily != "" {
				if got := pool.nodeSelector[consts.RBLNNPUFamilyLabelKey]; got != tc.wantFamily {
					t.Fatalf("nodeSelector family = %q, want %q", got, tc.wantFamily)
				}
				if pool.name != tc.wantFamily+"-ubuntu22.04-5.15.0-100-generic" {
					t.Fatalf("pool.name = %q, want family-prefixed", pool.name)
				}
				return
			}
			if _, exists := pool.nodeSelector[consts.RBLNNPUFamilyLabelKey]; exists {
				t.Fatalf("nodeSelector must not carry the family key for an invalid value: %v", pool.nodeSelector)
			}
			if pool.name != "ubuntu22.04-5.15.0-100-generic" {
				t.Fatalf("pool.name = %q, want unprefixed (invalid family treated as missing)", pool.name)
			}
		})
	}
}
