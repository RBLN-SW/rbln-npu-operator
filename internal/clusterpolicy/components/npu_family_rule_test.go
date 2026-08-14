package components

import (
	"context"
	"reflect"
	"strings"
	"testing"

	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	upstreamNFRGroup  = "nfd.k8s-sigs.io"
	openshiftNFRGroup = "nfd.openshift.io"
)

// familyRuleFixture builds a fake client whose RESTMapper serves exactly the
// given NodeFeatureRule groups, mirroring which NFD CRDs are installed.
func familyRuleFixture(t *testing.T, scopes map[string]meta.RESTScope) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := newTestScheme(t)
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range nodeFeatureRuleGVKs {
		scope, served := scopes[gvk.Group]
		if !served {
			continue
		}
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
		mapper.Add(gvk, scope)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper).Build()
	return c, s
}

func newFamilyRulePatcher(c client.Client, s *runtime.Scheme) Patcher {
	return NewNPUFamilyRulePatcher(c, logf.Log, testNamespace, s, "")
}

func getFamilyRule(t *testing.T, c client.Client, group, namespace string) (*unstructured.Unstructured, error) {
	t.Helper()
	rule := &unstructured.Unstructured{}
	for _, gvk := range nodeFeatureRuleGVKs {
		if gvk.Group == group {
			rule.SetGroupVersionKind(gvk)
		}
	}
	key := client.ObjectKey{Name: consts.RBLNBaseName + "-" + consts.RBLNNPUFamilyRuleName, Namespace: namespace}
	return rule, c.Get(context.Background(), key, rule)
}

func TestNPUFamilyRulePatcher_Patch(t *testing.T) {
	tests := map[string]struct {
		scopes        map[string]meta.RESTScope
		wantNamespace map[string]string // group -> expected namespace
	}{
		"upstream only": {
			scopes:        map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot},
			wantNamespace: map[string]string{upstreamNFRGroup: ""},
		},
		"openshift namespaced variant": {
			scopes:        map[string]meta.RESTScope{openshiftNFRGroup: meta.RESTScopeNamespace},
			wantNamespace: map[string]string{openshiftNFRGroup: testNamespace},
		},
		"both groups served": {
			scopes: map[string]meta.RESTScope{
				upstreamNFRGroup:  meta.RESTScopeRoot,
				openshiftNFRGroup: meta.RESTScopeRoot,
			},
			wantNamespace: map[string]string{upstreamNFRGroup: "", openshiftNFRGroup: ""},
		},
		"no NodeFeatureRule API served": {
			scopes:        map[string]meta.RESTScope{},
			wantNamespace: map[string]string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c, s := familyRuleFixture(t, tc.scopes)
			owner := newTestOwner()

			if err := newFamilyRulePatcher(c, s).Patch(context.Background(), owner); err != nil {
				t.Fatalf("Patch failed: %v", err)
			}

			for group, wantNS := range tc.wantNamespace {
				rule, err := getFamilyRule(t, c, group, wantNS)
				if err != nil {
					t.Fatalf("expected NodeFeatureRule in group %s (ns %q): %v", group, wantNS, err)
				}
				refs := rule.GetOwnerReferences()
				if len(refs) != 1 || refs[0].Name != owner.Name || refs[0].Controller == nil || !*refs[0].Controller {
					t.Errorf("group %s: want single controller ownerReference to %s, got %+v", group, owner.Name, refs)
				}
			}
		})
	}
}

// TestNPUFamilyRuleSpec pins the rendered rule content — above all the
// device-ID → family table — against an independently written literal.
func TestNPUFamilyRuleSpec(t *testing.T) {
	want := map[string]any{
		"rules": []any{
			map[string]any{
				"name":   "rbln-family-atom",
				"labels": map[string]any{"rebellions.ai/npu.family": "atom"},
				"matchFeatures": []any{
					map[string]any{
						"feature": "pci.device",
						"matchExpressions": map[string]any{
							"vendor": map[string]any{"op": "In", "value": []any{"1eff"}},
							"device": map[string]any{"op": "In", "value": []any{"1220", "1221", "1250", "1251"}},
						},
					},
				},
			},
			map[string]any{
				"name":   "rbln-family-rebel100",
				"labels": map[string]any{"rebellions.ai/npu.family": "rebel100"},
				"matchFeatures": []any{
					map[string]any{
						"feature": "pci.device",
						"matchExpressions": map[string]any{
							"vendor": map[string]any{"op": "In", "value": []any{"1eff"}},
							"device": map[string]any{"op": "In", "value": []any{"2030", "2031", "2130", "2131"}},
						},
					},
				},
			},
		},
	}
	if got := npuFamilyRuleSpec(); !reflect.DeepEqual(got, want) {
		t.Errorf("npuFamilyRuleSpec() = %#v, want %#v", got, want)
	}
}

func TestNPUFamilyRulePatcher_PatchIsIdempotent(t *testing.T) {
	c, s := familyRuleFixture(t, map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot})
	p := newFamilyRulePatcher(c, s)
	owner := newTestOwner()

	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("first Patch failed: %v", err)
	}
	first, err := getFamilyRule(t, c, upstreamNFRGroup, "")
	if err != nil {
		t.Fatalf("rule not found after first Patch: %v", err)
	}

	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("second Patch failed: %v", err)
	}
	second, err := getFamilyRule(t, c, upstreamNFRGroup, "")
	if err != nil {
		t.Fatalf("rule not found after second Patch: %v", err)
	}

	if first.GetResourceVersion() != second.GetResourceVersion() {
		t.Errorf("second Patch was not a no-op: resourceVersion %s -> %s",
			first.GetResourceVersion(), second.GetResourceVersion())
	}
}

// TestNPUFamilyRulePatcher_AdoptsRuleFromReplacedPolicy pins the adoption
// path: a policy deleted and recreated (new UID) must take over the rule its
// predecessor left behind instead of wedging on AlreadyOwnedError while
// garbage collection lags.
func TestNPUFamilyRulePatcher_AdoptsRuleFromReplacedPolicy(t *testing.T) {
	c, s := familyRuleFixture(t, map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot})
	p := newFamilyRulePatcher(c, s)

	oldOwner := newTestOwner()
	oldOwner.UID = "replaced-policy-uid"
	if err := p.Patch(context.Background(), oldOwner); err != nil {
		t.Fatalf("Patch with original policy failed: %v", err)
	}

	newOwner := newTestOwner()
	if err := p.Patch(context.Background(), newOwner); err != nil {
		t.Fatalf("Patch after policy replacement failed: %v", err)
	}

	rule, err := getFamilyRule(t, c, upstreamNFRGroup, "")
	if err != nil {
		t.Fatalf("rule not found: %v", err)
	}
	refs := rule.GetOwnerReferences()
	if len(refs) != 1 || refs[0].UID != newOwner.UID {
		t.Errorf("want single controller ref owned by UID %s, got %+v", newOwner.UID, refs)
	}
}

// A rule controlled by something other than an RBLNClusterPolicy stays
// untouched -- adopting it would fight its controller and GC-delete it on
// policy deletion -- and IsReady names the conflict instead.
func TestNPUFamilyRulePatcher_LeavesForeignControlledRuleAlone(t *testing.T) {
	c, s := familyRuleFixture(t, map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot})

	isController := true
	foreignRef := metav1.OwnerReference{
		APIVersion: "example.com/v1",
		Kind:       "SomeoneElsesOperator",
		Name:       "theirs",
		UID:        "foreign-uid",
		Controller: &isController,
	}
	foreign := &unstructured.Unstructured{}
	for _, gvk := range nodeFeatureRuleGVKs {
		if gvk.Group == upstreamNFRGroup {
			foreign.SetGroupVersionKind(gvk)
		}
	}
	foreign.SetName(consts.RBLNBaseName + "-" + consts.RBLNNPUFamilyRuleName)
	foreign.SetOwnerReferences([]metav1.OwnerReference{foreignRef})
	userSpec := map[string]any{"rules": []any{map[string]any{"name": "user-authored"}}}
	if err := unstructured.SetNestedField(foreign.Object, userSpec, "spec"); err != nil {
		t.Fatalf("SetNestedField: %v", err)
	}
	if err := c.Create(context.Background(), foreign); err != nil {
		t.Fatalf("create foreign rule: %v", err)
	}

	p := newFamilyRulePatcher(c, s)
	owner := newTestOwner()
	// Isolation: a user-owned rule must not abort PatchComponents for every
	// other operand.
	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	rule, err := getFamilyRule(t, c, upstreamNFRGroup, "")
	if err != nil {
		t.Fatalf("rule not found: %v", err)
	}
	refs := rule.GetOwnerReferences()
	if len(refs) != 1 || refs[0].UID != foreignRef.UID {
		t.Errorf("foreign controller ref must survive untouched, got %+v", refs)
	}
	spec, _, err := unstructured.NestedMap(rule.Object, "spec")
	if err != nil {
		t.Fatalf("NestedMap(spec): %v", err)
	}
	if !reflect.DeepEqual(spec, userSpec) {
		t.Errorf("user spec must survive untouched, got %+v", spec)
	}

	report := p.IsReady(context.Background(), 0)
	if report.State != rblnv1beta1.ComponentStateNotReady {
		t.Errorf("IsReady().State = %q, want %q", report.State, rblnv1beta1.ComponentStateNotReady)
	}
	if !strings.Contains(report.Message, foreignRef.Kind) || !strings.Contains(report.Message, foreignRef.Name) {
		t.Errorf("IsReady().Message = %q, want it to name the foreign controller %s %q",
			report.Message, foreignRef.Kind, foreignRef.Name)
	}
}

// Missing RBAC for a served NodeFeatureRule group is environmental, like a
// missing CRD: it degrades to this component's notReady rather than aborting
// every remaining operand's patch.
func TestNPUFamilyRulePatcher_ForbiddenRBACDoesNotAbortPatch(t *testing.T) {
	s := newTestScheme(t)
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range nodeFeatureRuleGVKs {
		if gvk.Group != upstreamNFRGroup {
			continue
		}
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
		mapper.Add(gvk, meta.RESTScopeRoot)
	}
	forbid := func(gvk schema.GroupVersionKind) error {
		return kapierrors.NewForbidden(
			schema.GroupResource{Group: gvk.Group, Resource: "nodefeaturerules"}, "", nil)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind().Group == upstreamNFRGroup {
					return forbid(u.GroupVersionKind())
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	p := newFamilyRulePatcher(c, s)
	if err := p.Patch(context.Background(), newTestOwner()); err != nil {
		t.Fatalf("Patch() must isolate a Forbidden NFR group, got error: %v", err)
	}

	report := p.IsReady(context.Background(), 0)
	if report.State != rblnv1beta1.ComponentStateNotReady {
		t.Errorf("IsReady().State = %q, want %q", report.State, rblnv1beta1.ComponentStateNotReady)
	}
}

func TestNPUFamilyRulePatcher_IsReady(t *testing.T) {
	t.Run("no API served", func(t *testing.T) {
		c, s := familyRuleFixture(t, map[string]meta.RESTScope{})
		report := newFamilyRulePatcher(c, s).IsReady(context.Background(), 0)
		if report.State != rblnv1beta1.ComponentStateNotReady {
			t.Errorf("want state %s, got %s", rblnv1beta1.ComponentStateNotReady, report.State)
		}
		if !strings.Contains(report.Message, "install NFD") {
			t.Errorf("want actionable message mentioning NFD install, got %q", report.Message)
		}
	})

	t.Run("API served but rule missing", func(t *testing.T) {
		c, s := familyRuleFixture(t, map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot})
		report := newFamilyRulePatcher(c, s).IsReady(context.Background(), 0)
		if report.State != rblnv1beta1.ComponentStateNotReady {
			t.Errorf("want state %s, got %s", rblnv1beta1.ComponentStateNotReady, report.State)
		}
		if !strings.Contains(report.Message, upstreamNFRGroup) {
			t.Errorf("want message naming the group with the missing rule, got %q", report.Message)
		}
	})

	t.Run("rule present in every served group", func(t *testing.T) {
		c, s := familyRuleFixture(t, map[string]meta.RESTScope{
			upstreamNFRGroup:  meta.RESTScopeRoot,
			openshiftNFRGroup: meta.RESTScopeRoot,
		})
		p := newFamilyRulePatcher(c, s)
		if err := p.Patch(context.Background(), newTestOwner()); err != nil {
			t.Fatalf("Patch failed: %v", err)
		}
		report := p.IsReady(context.Background(), 0)
		if report.State != rblnv1beta1.ComponentStateReady || report.Desired != 2 || report.Ready != 2 {
			t.Errorf("want ready 2/2, got %s %d/%d", report.State, report.Ready, report.Desired)
		}
	})
}

func TestNPUFamilyRulePatcher_CleanUp(t *testing.T) {
	c, s := familyRuleFixture(t, map[string]meta.RESTScope{upstreamNFRGroup: meta.RESTScopeRoot})
	p := newFamilyRulePatcher(c, s)
	owner := newTestOwner()

	if err := p.CleanUp(context.Background(), owner); err != nil {
		t.Fatalf("CleanUp with nothing to delete failed: %v", err)
	}

	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatalf("Patch failed: %v", err)
	}
	if err := p.CleanUp(context.Background(), owner); err != nil {
		t.Fatalf("CleanUp failed: %v", err)
	}
	if _, err := getFamilyRule(t, c, upstreamNFRGroup, ""); err == nil {
		t.Error("NodeFeatureRule still exists after CleanUp")
	}
}

func TestNPUFamilyRulePatcher_AlwaysEnabled(t *testing.T) {
	c, s := familyRuleFixture(t, map[string]meta.RESTScope{})
	p := newFamilyRulePatcher(c, s)
	if !p.IsEnabled() {
		t.Error("npu-family rule component must be always enabled")
	}
	if got, want := p.ComponentName(), "rbln-npu-family"; got != want {
		t.Errorf("ComponentName() = %q, want %q", got, want)
	}
}
