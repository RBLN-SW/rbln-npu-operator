package components

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/registry"
)

func TestNewDriverManagerPatcher_NilDriver(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	_, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, nil, scheme, &fakeChecker{}, "", nil)
	if err == nil {
		t.Fatal("expected error for nil driver, got nil")
	}
}

func TestNewDriverManagerPatcher_NilChecker(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	_, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, newTestOwner(), scheme, nil, "", nil)
	if err == nil {
		t.Fatal("expected error for nil checker, got nil")
	}
}

func TestDriverManagerPatcher_Patch(t *testing.T) {
	scheme := newTestScheme(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNNPUFamilyLabelKey:                              "atom",
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}
	c := newFakeClient(t, scheme, node)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount — ownerRef to driver instance
	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, sa)
	assertHasOwnerRef(t, sa, owner.Name)

	// ClusterRole
	cr := &rbacv1.ClusterRole{}
	assertClusterObjectExists(t, c, driverManagerName, cr)
	assertClusterRoleHasRule(t, cr, "", "nodes")
	assertClusterRoleHasRule(t, cr, "", "pods")
	assertClusterRoleHasRule(t, cr, "apps", "daemonsets")

	// ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	assertClusterObjectExists(t, c, driverManagerName, crb)

	// ConfigMap (startup probe)
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName + "-" + startupProbeConfigMapSuffix, Namespace: testNamespace}, cm)
	if _, ok := cm.Data[startupProbeScriptName]; !ok {
		t.Fatalf("ConfigMap missing key %q", startupProbeScriptName)
	}
}

func TestDriverManagerPatcher_Patch_OpenShift(t *testing.T) {
	scheme := newTestScheme(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-ocp",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNNPUFamilyLabelKey:                              "atom",
				"feature.node.kubernetes.io/system-os_release.ID":         "rhcos",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "4.14",
				"feature.node.kubernetes.io/kernel-version.full":          "5.14.0-284.el9.x86_64",
			},
		},
	}
	c := newFakeClient(t, scheme, node)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "v4.14.0", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// Only the wiring: NewDriverManagerPatcher must thread openshiftVersion
	// into the basePatcher so Patch reaches reconcileOpenShiftRBAC at all.
	// The RBAC contents are pinned by TestReconcileOpenShiftRBAC.
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &rbacv1.Role{})
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &rbacv1.RoleBinding{})
}

func TestDriverManagerPatcher_Patch_NoMatchingNodesDeletesStaleDaemonSets(t *testing.T) {
	scheme := newTestScheme(t)

	// Existing DaemonSet from a previous reconcile (e.g. before the node got
	// the `rebellions.ai/npu.deploy.skip` label). The owner-managed controller
	// has since removed the `rebellions.ai/npu.deploy.driver` label, so no
	// nodes match the driver selector anymore.
	existingDS := newStaleTestDaemonSet(
		testInstanceName+"-ubuntu22.04-5.15.0-100-generic",
		testInstanceName,
		"ubuntu22.04-5.15.0-100-generic",
	)

	// Node is present in the cluster but no longer carries the deploy.driver
	// label (simulating the skip-label cleanup by RBLNClusterPolicy).
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-skip",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.skip":                           "true",
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}

	c := newFakeClient(t, scheme, node, existingDS)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", nil)
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// The stale DaemonSet must be deleted even though no node pools were
	// detected. Otherwise driver pods on skipped nodes stay running.
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      existingDS.Name,
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})
}

// An owned node that lacks npu.family must not be read as "no owned nodes":
// that would delete every DaemonSet for this instance, uninstalling the
// driver fleet-wide on every label gap.
func TestDriverManagerPatcher_Patch_KeepsExistingDaemonSetsWhenOwnedNodesLackFamily(t *testing.T) {
	scheme := newTestScheme(t)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-label-gap",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNDriverOwnerLabelKey:                            testInstanceName,
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}

	// Simulates a DaemonSet created by an earlier, successful reconcile
	// before this node's family label disappeared (or was invalidated).
	existingDS := newStaleTestDaemonSet(
		testInstanceName+"-atom-ubuntu22.04-5.15.0-100-generic",
		testInstanceName,
		"atom-ubuntu22.04-5.15.0-100-generic",
	)

	c := newFakeClient(t, scheme, node, existingDS)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectExists(t, c, types.NamespacedName{
		Name:      existingDS.Name,
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})
}

// One node lacking npu.family must not disturb a pool that still has valid
// nodes: its DaemonSet stays. The reap is held back for the whole pass though,
// so even a stale DaemonSet that is provably serving nobody survives until the
// label gap heals -- deliberately coarser than proving per-DaemonSet whether it
// still covers an unrenderable node, because a stale pool has no matching nodes
// and therefore runs zero pods, making one extra pass of delay free.
func TestDriverManagerPatcher_Patch_MixedFamilyKeepsValidPoolDaemonSet(t *testing.T) {
	scheme := newTestScheme(t)

	labeledNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-labeled",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNDriverOwnerLabelKey:                            testInstanceName,
				consts.RBLNNPUFamilyLabelKey:                              "atom",
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}
	unlabeledNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-unlabeled",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNDriverOwnerLabelKey:                            testInstanceName,
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "6.8.0-45-generic",
			},
		},
	}

	validPoolDS := newStaleTestDaemonSet(
		testInstanceName+"-atom-ubuntu22.04-5.15.0-100-generic",
		testInstanceName,
		"atom-ubuntu22.04-5.15.0-100-generic",
	)
	stalePoolDS := newStaleTestDaemonSet(
		testInstanceName+"-atom-rhel9-5.14.0",
		testInstanceName,
		"atom-rhel9-5.14.0",
	)
	// The real selector an operator-rendered rhel9 pool carries: it matches
	// neither node above, so this DaemonSet runs no pods while it is retained.
	stalePoolDS.Spec.Template.Spec.NodeSelector = map[string]string{
		driverManagerDeployLabelKey:    "true",
		consts.RBLNDriverOwnerLabelKey: testInstanceName,
		consts.RBLNNPUFamilyLabelKey:   "atom",
		consts.NFDOSReleaseIDLabelKey:  "rhel",
		consts.NFDOSVersionIDLabelKey:  "9",
		consts.NFDKernelLabelKey:       "5.14.0",
	}

	c := newFakeClient(t, scheme, labeledNode, unlabeledNode, validPoolDS, stalePoolDS)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*labeledNode, *unlabeledNode})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectExists(t, c, types.NamespacedName{
		Name:      validPoolDS.Name,
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})
	assertObjectExists(t, c, types.NamespacedName{
		Name:      stalePoolDS.Name,
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})

	if got, want := p.Diagnostics().NodesWithoutFamily, []string{"worker-unlabeled"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Diagnostics().NodesWithoutFamily = %v, want %v", got, want)
	}
}

// A missing image for one family must not hold back a sibling family's first
// DaemonSet: with nothing stale to collide with, there is no reason to wait.
func TestDriverManagerPatcher_Patch_MissingImageDoesNotBlockSiblingPoolCreate(t *testing.T) {
	scheme := newTestScheme(t)
	nodeA := familyNode("worker-a", "atom")
	nodeB := familyNode("worker-b", "rebel100")

	c := newFakeClient(t, scheme, nodeA, nodeB)
	ctx := context.Background()
	owner := newTestOwner()

	refA, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath(atom): %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{refA: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*nodeA, *nodeB})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("rebel100"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})
}

// PoolStatuses feeds DriverService.AssembleStatus, which is what the
// reconciler renders into .status and the Ready condition, so the
// Ready/Progressing classification and the instance filter are pinned here.
func TestDriverManagerPatcher_PoolStatuses(t *testing.T) {
	tests := map[string]struct {
		existingDS []*appsv1.DaemonSet
		wantStates map[string]rebellionsaiv1alpha1.DriverPoolState
	}{
		"no DaemonSets yields no pools": {
			existingDS: nil,
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{},
		},
		"single DaemonSet all pods ready": {
			existingDS: []*appsv1.DaemonSet{
				newTestDaemonSetWithStatus(testInstanceName+"-pool1", testInstanceName, "pool1", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 2,
					NumberReady:            2,
					NumberUnavailable:      0,
				}),
			},
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{
				testInstanceName + "-pool1": rebellionsaiv1alpha1.DriverPoolStateReady,
			},
		},
		"single DaemonSet some pods not ready": {
			existingDS: []*appsv1.DaemonSet{
				newTestDaemonSetWithStatus(testInstanceName+"-pool1", testInstanceName, "pool1", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 3,
					NumberReady:            1,
					NumberUnavailable:      2,
				}),
			},
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{
				testInstanceName + "-pool1": rebellionsaiv1alpha1.DriverPoolStateProgressing,
			},
		},
		"zero desired is progressing, not ready": {
			existingDS: []*appsv1.DaemonSet{
				newTestDaemonSetWithStatus(testInstanceName+"-pool1", testInstanceName, "pool1", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 0,
					NumberReady:            0,
					NumberUnavailable:      0,
				}),
			},
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{
				testInstanceName + "-pool1": rebellionsaiv1alpha1.DriverPoolStateProgressing,
			},
		},
		"multiple DaemonSets are classified independently": {
			existingDS: []*appsv1.DaemonSet{
				newTestDaemonSetWithStatus(testInstanceName+"-pool1", testInstanceName, "pool1", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 1,
					NumberReady:            1,
					NumberUnavailable:      0,
				}),
				newTestDaemonSetWithStatus(testInstanceName+"-pool2", testInstanceName, "pool2", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 2,
					NumberReady:            0,
					NumberUnavailable:      2,
				}),
			},
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{
				testInstanceName + "-pool1": rebellionsaiv1alpha1.DriverPoolStateReady,
				testInstanceName + "-pool2": rebellionsaiv1alpha1.DriverPoolStateProgressing,
			},
		},
		"ignores other instances": {
			existingDS: []*appsv1.DaemonSet{
				newTestDaemonSetWithStatus("other-instance-pool1", "other-instance", "pool1", appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 2,
					NumberReady:            0,
					NumberUnavailable:      2,
				}),
			},
			wantStates: map[string]rebellionsaiv1alpha1.DriverPoolState{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := newFakeClientWithDS(t, scheme, tc.existingDS)

			h := &driverManagerPatcher{
				basePatcher: basePatcher{
					client:       c,
					apiReader:    c,
					log:          logf.Log,
					name:         driverManagerName,
					instanceName: testInstanceName,
					namespace:    testNamespace,
				},
			}

			pools, err := h.PoolStatuses(context.Background())
			if err != nil {
				t.Fatalf("PoolStatuses() error: %v", err)
			}
			if len(pools) != len(tc.wantStates) {
				t.Fatalf("PoolStatuses() returned %d pools, want %d: %+v", len(pools), len(tc.wantStates), pools)
			}
			for _, p := range pools {
				want, ok := tc.wantStates[p.Name]
				if !ok {
					t.Fatalf("unexpected pool %q in %+v", p.Name, pools)
				}
				if p.State != want {
					t.Errorf("pool %q state = %q, want %q (desired=%d ready=%d)", p.Name, p.State, want, p.Desired, p.Ready)
				}
			}
		})
	}
}

func newTestDaemonSetWithStatus(name, instanceName, poolName string, status appsv1.DaemonSetStatus) *appsv1.DaemonSet {
	ds := newStaleTestDaemonSet(name, instanceName, poolName)
	ds.Status = status
	return ds
}

// ---------------------------------------------------------------------------
// ImageChecker fake + registry fast-fail tests
// ---------------------------------------------------------------------------

// fakeChecker is a test double for ImageChecker. The zero value returns
// VerdictExists for every ref not listed in verdicts, so tests unrelated to
// registry checking see the same DaemonSets they would if no check were
// wired in at all.
type fakeChecker struct {
	verdicts map[string]registry.Verdict
	calls    []string
	// secretsLens records len(pullSecrets) for each Check call, same index
	// as calls, so pull-secret tests can assert on it without comparing
	// corev1.Secret values.
	secretsLens []int
}

func (f *fakeChecker) Check(_ context.Context, imageRef string, pullSecrets []corev1.Secret) (registry.Verdict, error) {
	f.calls = append(f.calls, imageRef)
	f.secretsLens = append(f.secretsLens, len(pullSecrets))

	v, ok := f.verdicts[imageRef]
	if !ok {
		v = registry.VerdictExists
	}
	if v == registry.VerdictExists || v == registry.VerdictSkipped {
		return v, nil
	}
	return v, fmt.Errorf("fakeChecker: %s verdict for %s", v, imageRef)
}

// familyNode builds an owned-node fixture shaped as the resolver hands it to
// the patcher: this instance's owner label, the given npu.family, and a
// fixed ubuntu22.04/5.15.0-100-generic OS/kernel pair.
func familyNode(name, family string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				consts.RBLNDriverOwnerLabelKey:                            testInstanceName,
				consts.RBLNNPUFamilyLabelKey:                              family,
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}
}

func familyPoolName(family string) string {
	return family + "-ubuntu22.04-5.15.0-100-generic"
}

// legacyPoolName and legacyNodeSelector fix the same ubuntu22.04/kernel pair
// familyNode/familyPoolName use, but in the pre-family-label shape: no
// npu.family key anywhere, and no family segment in the pool name.
const legacyPoolName = "ubuntu22.04-5.15.0-100-generic"

// legacyDSName is the DaemonSet name every legacy-DaemonSet scenario in this
// file uses: one instance, one (os,kernel) pair.
const legacyDSName = testInstanceName + "-" + legacyPoolName

// legacyNodeSelector reconstructs the pod-template nodeSelector a
// pre-family-label DaemonSet carries: every key buildNodePool sets except
// npu.family.
func legacyNodeSelector(kernel string) map[string]string {
	return map[string]string{
		driverManagerDeployLabelKey:    "true",
		consts.RBLNDriverOwnerLabelKey: testInstanceName,
		consts.NFDOSReleaseIDLabelKey:  "ubuntu",
		consts.NFDOSVersionIDLabelKey:  "22.04",
		consts.NFDKernelLabelKey:       kernel,
	}
}

// newLegacyTestDaemonSet builds a pre-family-label DaemonSet fixture: same
// shape newStaleTestDaemonSet produces, plus a pod-template nodeSelector
// with no npu.family key.
func newLegacyTestDaemonSet() *appsv1.DaemonSet {
	ds := newStaleTestDaemonSet(legacyDSName, testInstanceName, legacyPoolName)
	ds.Spec.Template.Spec.NodeSelector = legacyNodeSelector("5.15.0-100-generic")
	return ds
}

// Only the pull secrets that actually exist are resolved, and the resolved set
// -- not the configured names -- is what reaches Check.
func TestDriverManagerPatcher_Patch_PassesOnlyResolvablePullSecrets(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-missing-secret", "atom")
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "real-secret", Namespace: testNamespace},
	}

	c := newFakeClient(t, scheme, node, existing)
	ctx := context.Background()
	owner := newTestOwner()
	owner.Spec.ImagePullSecrets = []string{"ghost-secret", "real-secret"}

	checker := &fakeChecker{}
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if len(checker.secretsLens) == 0 {
		t.Fatal("checker.Check() was never called")
	}
	for i, n := range checker.secretsLens {
		if n != 1 {
			t.Fatalf("Check() call %d received %d pull secrets, want 1 (existing secret passed, missing one skipped)", i, n)
		}
	}
}

// A 404 blocks the pool regardless of whether the check could authenticate: a
// referenced-but-missing secret breaks kubelet's pull too, so the verdict is
// the same either way. The unreadable secret is reported alongside it, because
// it is the one thing that can make a 404 wrong (a registry answering 404
// rather than 401/403 to unauthorized reads) and the condition message has to
// say so instead of asserting the image is absent.
func TestDriverManagerPatcher_Patch_UnreadablePullSecretStillBlocksAndIsReported(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-unauthed-404", "atom")

	c := newFakeClient(t, scheme, node)
	ctx := context.Background()
	owner := newTestOwner()
	owner.Spec.ImagePullSecrets = []string{"ghost-secret"}

	ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath: %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("atom"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})

	diag := p.Diagnostics()
	want := []MissingImagePool{{Pool: familyPoolName("atom"), Image: ref}}
	if !reflect.DeepEqual(diag.MissingImagePools, want) {
		t.Fatalf("Diagnostics().MissingImagePools = %+v, want %+v", diag.MissingImagePools, want)
	}
	if len(diag.UnreadablePullSecrets) != 1 || !strings.HasPrefix(diag.UnreadablePullSecrets[0], "ghost-secret (") {
		t.Fatalf("Diagnostics().UnreadablePullSecrets = %v, want [ghost-secret (<reason>)]", diag.UnreadablePullSecrets)
	}
}

// A pool whose image is missing yields no DaemonSet and is surfaced via
// Diagnostics(), without failing the whole Patch call for sibling pools.
func TestDriverManagerPatcher_Patch_FastFailsMissingImagePool(t *testing.T) {
	scheme := newTestScheme(t)
	nodeA := familyNode("worker-a", "atom")
	nodeB := familyNode("worker-b", "rebel100")

	c := newFakeClient(t, scheme, nodeA, nodeB)
	ctx := context.Background()
	owner := newTestOwner()

	refA, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath(atom): %v", err)
	}
	refB, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "rebel100")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath(rebel100): %v", err)
	}

	checker := &fakeChecker{verdicts: map[string]registry.Verdict{refA: registry.VerdictNotFound}}
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*nodeA, *nodeB})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("atom"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})
	assertObjectExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("rebel100"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})

	want := []MissingImagePool{{Pool: familyPoolName("atom"), Image: refA}}
	if got := p.Diagnostics().MissingImagePools; !reflect.DeepEqual(got, want) {
		t.Fatalf("Diagnostics().MissingImagePools = %+v, want %+v", got, want)
	}

	checkedB := false
	for _, ref := range checker.calls {
		if ref == refB {
			checkedB = true
		}
	}
	if !checkedB {
		t.Fatalf("checker.calls = %v, want a call for pool B's ref %q", checker.calls, refB)
	}
}

// A NotFound verdict on a pool whose DaemonSet already exists (version
// bumped to an unpublished tag) must neither reap nor update it, so the
// running driver keeps working.
func TestDriverManagerPatcher_Patch_KeepsExistingDaemonSetWhenImageMissing(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-missing-image", "atom")

	poolName := familyPoolName("atom")
	dsName := testInstanceName + "-" + poolName
	existingDS := newStaleTestDaemonSet(dsName, testInstanceName, poolName)

	c := newFakeClient(t, scheme, node, existingDS)
	ctx := context.Background()
	owner := newTestOwner()

	dsKey := types.NamespacedName{Name: dsName, Namespace: testNamespace}
	before := &appsv1.DaemonSet{}
	assertObjectExists(t, c, dsKey, before)

	ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath: %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	after := &appsv1.DaemonSet{}
	assertObjectExists(t, c, dsKey, after)
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("DaemonSet %s was modified (resourceVersion %s -> %s), want untouched",
			dsName, before.ResourceVersion, after.ResourceVersion)
	}
}

// Unknown (kubelet remains the final arbiter) and Skipped must still create
// the DaemonSet and must not populate MissingImagePools.
func TestDriverManagerPatcher_Patch_NonBlockingVerdictsStillCreateDaemonSet(t *testing.T) {
	tests := map[string]registry.Verdict{
		"unknown verdict proceeds": registry.VerdictUnknown,
		"skipped verdict proceeds": registry.VerdictSkipped,
	}

	for name, verdict := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			node := familyNode("worker-nonblocking", "atom")

			c := newFakeClient(t, scheme, node)
			ctx := context.Background()
			owner := newTestOwner()

			ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
			if err != nil {
				t.Fatalf("GetPrecompiledImagePath: %v", err)
			}
			checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: verdict}}

			p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
			if err != nil {
				t.Fatalf("NewDriverManagerPatcher() error: %v", err)
			}
			if err := p.Patch(ctx, owner); err != nil {
				t.Fatalf("Patch() error: %v", err)
			}

			assertObjectExists(t, c, types.NamespacedName{
				Name:      testInstanceName + "-" + familyPoolName("atom"),
				Namespace: testNamespace,
			}, &appsv1.DaemonSet{})

			if got := p.Diagnostics().MissingImagePools; len(got) != 0 {
				t.Fatalf("MissingImagePools = %+v, want empty", got)
			}
		})
	}
}

// A pool's recovery clears its stale diagnostic instead of accumulating it.
func TestDriverManagerPatcher_Patch_DiagnosticsResetBetweenPatches(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-recovers", "atom")

	c := newFakeClient(t, scheme, node)
	ctx := context.Background()
	owner := newTestOwner()

	ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath: %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("first Patch() error: %v", err)
	}
	if got := p.Diagnostics().MissingImagePools; len(got) == 0 {
		t.Fatal("want non-empty MissingImagePools after first Patch (NotFound)")
	}

	checker.verdicts[ref] = registry.VerdictExists
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("second Patch() error: %v", err)
	}
	if got := p.Diagnostics().MissingImagePools; len(got) != 0 {
		t.Fatalf("MissingImagePools after recovery = %+v, want empty (stale diagnostics must not leak)", got)
	}
}

// Diagnostics() must copy its slices: mutating the caller's copy must never
// be visible on a later call.
func TestDriverManagerPatcher_Diagnostics_ReturnsCopy(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-copy", "atom")

	c := newFakeClient(t, scheme, node)
	ctx := context.Background()
	owner := newTestOwner()

	ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath: %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	first := p.Diagnostics()
	if len(first.MissingImagePools) == 0 {
		t.Fatal("want non-empty MissingImagePools to exercise the copy")
	}
	first.MissingImagePools[0].Pool = "mutated"

	second := p.Diagnostics()
	if second.MissingImagePools[0].Pool == "mutated" {
		t.Fatal("Diagnostics() leaked internal state: mutating a returned slice affected a later call")
	}
}

// Pools are collected via a map, so their natural order is random: two Patch
// passes over an unchanged fleet must still yield the same MissingImagePools
// order, or the rendered condition message flaps and each flap costs a status
// write and a requeue. Three families make a coincidental pass unlikely.
func TestDriverManagerPatcher_Diagnostics_MissingImagePoolsSorted(t *testing.T) {
	scheme := newTestScheme(t)
	nodeZulu := familyNode("worker-zulu", "zulu")
	nodeAlpha := familyNode("worker-alpha", "alpha")
	nodeMike := familyNode("worker-mike", "mike")

	c := newFakeClient(t, scheme, nodeZulu, nodeAlpha, nodeMike)
	ctx := context.Background()
	owner := newTestOwner()

	verdicts := make(map[string]registry.Verdict, 3)
	for _, family := range []string{"zulu", "alpha", "mike"} {
		ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", family)
		if err != nil {
			t.Fatalf("GetPrecompiledImagePath(%s): %v", family, err)
		}
		verdicts[ref] = registry.VerdictNotFound
	}
	checker := &fakeChecker{verdicts: verdicts}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*nodeZulu, *nodeAlpha, *nodeMike})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	wantOrder := []string{familyPoolName("alpha"), familyPoolName("mike"), familyPoolName("zulu")}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("first Patch() error: %v", err)
	}
	first := p.Diagnostics().MissingImagePools
	if got := missingImagePoolNames(first); !reflect.DeepEqual(got, wantOrder) {
		t.Fatalf("first pass MissingImagePools order = %v, want %v", got, wantOrder)
	}

	// A second Patch over the identical fleet must yield the identical
	// order -- the pin for "two Patch passes over the same fleet yield
	// identical Diagnostics().MissingImagePools ordering".
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("second Patch() error: %v", err)
	}
	second := p.Diagnostics().MissingImagePools
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("second pass MissingImagePools = %+v, want identical to first pass %+v", second, first)
	}
}

func missingImagePoolNames(pools []MissingImagePool) []string {
	names := make([]string, len(pools))
	for i, p := range pools {
		names[i] = p.Pool
	}
	return names
}

// A stale DaemonSet must not be reaped while a pool's successor cannot be
// rendered (image missing in the registry): on a pool-name transition --
// flat→family upgrade, or any future name-format change -- reaping first
// would tear down the running driver with nothing to replace it.
func TestDriverManagerPatcher_Patch_DefersReapWhenSuccessorImageMissing(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-defer-missing-image", "atom")
	legacyDS := newLegacyTestDaemonSet()
	c := newFakeClient(t, scheme, node, legacyDS)
	ctx := context.Background()
	owner := newTestOwner()

	ref, err := owner.Spec.GetPrecompiledImagePath("ubuntu22.04", "5.15.0-100-generic", "atom")
	if err != nil {
		t.Fatalf("GetPrecompiledImagePath: %v", err)
	}
	checker := &fakeChecker{verdicts: map[string]registry.Verdict{ref: registry.VerdictNotFound}}

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, checker, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectExists(t, c, types.NamespacedName{Name: legacyDSName, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("atom"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})

	want := []MissingImagePool{{Pool: familyPoolName("atom"), Image: ref}}
	if got := p.Diagnostics().MissingImagePools; !reflect.DeepEqual(got, want) {
		t.Fatalf("Diagnostics().MissingImagePools = %+v, want %+v", got, want)
	}
}

// A stale DaemonSet must not be reaped while it still covers an owned node
// whose pool is unresolvable (no usable npu.family label): it may be the only
// driver that node has. And while it is retained, the family pool it covers
// must not be created either -- the retained DaemonSet still schedules onto
// the same nodes, and two driver installers on one host is the failure mode
// the reap ordering exists to prevent.
func TestDriverManagerPatcher_Patch_DefersReapWhenNodesLackFamilyLabel(t *testing.T) {
	scheme := newTestScheme(t)
	labeled := familyNode("worker-defer-labeled", "atom")
	unlabeled := familyNode("worker-defer-unlabeled", "atom")
	delete(unlabeled.Labels, consts.RBLNNPUFamilyLabelKey)
	legacyDS := newLegacyTestDaemonSet()

	c := newFakeClient(t, scheme, labeled, unlabeled, legacyDS)
	ctx := context.Background()
	owner := newTestOwner()

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*labeled, *unlabeled})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectExists(t, c, types.NamespacedName{Name: legacyDSName, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      testInstanceName + "-" + familyPoolName("atom"),
		Namespace: testNamespace,
	}, &appsv1.DaemonSet{})

	if got, want := p.Diagnostics().NodesWithoutFamily, []string{"worker-defer-unlabeled"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Diagnostics().NodesWithoutFamily = %v, want %v", got, want)
	}
}

// A DaemonSet whose pool name carries no family segment gets no special
// handling: once every pool is renderable it is reaped like any other stale
// one, and its replacement is created in the same pass.
func TestDriverManagerPatcher_Patch_FlatLegacyDaemonSetIsOrdinaryStale(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-flat-legacy", "atom")
	legacyDS := newLegacyTestDaemonSet()
	c := newFakeClient(t, scheme, node, legacyDS)
	ctx := context.Background()
	owner := newTestOwner()

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: legacyDSName, Namespace: testNamespace}, &appsv1.DaemonSet{})
	familyKey := types.NamespacedName{Name: testInstanceName + "-" + familyPoolName("atom"), Namespace: testNamespace}
	assertObjectExists(t, c, familyKey, &appsv1.DaemonSet{})
}

// Reaps must use foreground propagation: with the API default the object
// vanishes while its pods are still terminating, so an in-progress reap
// reads as finished.
func TestDriverManagerPatcher_Patch_ReapsDaemonSetsWithForegroundDeletion(t *testing.T) {
	scheme := newTestScheme(t)
	legacyDS := newLegacyTestDaemonSet()

	var dsDeletePolicies []string
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(legacyDS).
		WithStatusSubresource(&appsv1.DaemonSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, isDS := obj.(*appsv1.DaemonSet); isDS {
					applied := &client.DeleteOptions{}
					applied.ApplyOptions(opts)
					policy := ""
					if applied.PropagationPolicy != nil {
						policy = string(*applied.PropagationPolicy)
					}
					dsDeletePolicies = append(dsDeletePolicies, policy)
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", nil)
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	// The resolver assigned this instance no nodes, so Patch reaps every
	// DaemonSet through cleanUpStaleDaemonSets.
	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	want := []string{string(metav1.DeletePropagationForeground)}
	if !reflect.DeepEqual(dsDeletePolicies, want) {
		t.Fatalf("DaemonSet delete propagation policies = %v, want %v", dsDeletePolicies, want)
	}
}

// A composed DaemonSet name that collides with one labeled for a different
// instance must error, never adopt or update it.
func TestDriverManagerPatcher_Patch_RefusesCrossInstanceDaemonSetCollision(t *testing.T) {
	scheme := newTestScheme(t)
	node := familyNode("worker-collision", "atom")

	const siblingInstance = "rbln-driver-atom"
	collidingName := testInstanceName + "-" + familyPoolName("atom")
	collidingDS := newStaleTestDaemonSet(collidingName, siblingInstance, "some-other-pool")

	c := newFakeClient(t, scheme, node, collidingDS)
	ctx := context.Background()
	owner := newTestOwner()

	dsKey := types.NamespacedName{Name: collidingName, Namespace: testNamespace}
	before := &appsv1.DaemonSet{}
	assertObjectExists(t, c, dsKey, before)

	p, err := NewDriverManagerPatcher(c, c, logf.Log, testNamespace, owner, scheme, &fakeChecker{}, "", []corev1.Node{*node})
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err == nil {
		t.Fatal("Patch() error = nil, want a collision error")
	}

	after := &appsv1.DaemonSet{}
	assertObjectExists(t, c, dsKey, after)
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("colliding DaemonSet was modified (resourceVersion %s -> %s), want untouched",
			before.ResourceVersion, after.ResourceVersion)
	}
	if got := after.Labels[driverManagerInstanceLabelKey]; got != siblingInstance {
		t.Fatalf("colliding DaemonSet instance label = %q, want unchanged %q", got, siblingInstance)
	}
}
