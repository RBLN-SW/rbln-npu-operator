package components

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// nodeFeatureRuleGVKs are the NodeFeatureRule APIs this patcher can target:
// upstream NFD and the Red Hat NFD Operator's fork. The rule is created in
// every group the cluster serves — which nfd-master runs is not observable
// from here, and a rule in an unserved group is inert. v1alpha1 is pinned
// because the spec builder renders that version's schema; auto-discovering a
// newer one would trade a visible notReady for a subtly wrong object.
var nodeFeatureRuleGVKs = []schema.GroupVersionKind{
	{Group: "nfd.k8s-sigs.io", Version: "v1alpha1", Kind: "NodeFeatureRule"},
	{Group: "nfd.openshift.io", Version: "v1alpha1", Kind: "NodeFeatureRule"},
}

// npuFamilyRulePatcher manages the NodeFeatureRule that labels NPU nodes with
// their product family (consts.RBLNNPUFamilyLabelKey). It is always enabled:
// the family label routes per-family RBLNDriver instances, so it must exist
// before any driver is installed — which also rules out deriving it from
// operands that need a running driver (npu-feature-discovery → rbln-smd).
type npuFamilyRulePatcher struct {
	basePatcher
}

func NewNPUFamilyRulePatcher(client client.Client, log logr.Logger, namespace string, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	return &npuFamilyRulePatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             consts.RBLNBaseName + "-" + consts.RBLNNPUFamilyRuleName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          true,
			workloadType:     consts.RBLNWorkloadConfigContainer,
		},
	}
}

// Patch creates or updates the rule in every served NodeFeatureRule API. A
// cluster serving none is not a Patch error: failing would abort every other
// component over a dependency only this one has. IsReady surfaces the gap,
// and installing NFD relabels nodes, which wakes the reconciler.
func (h *npuFamilyRulePatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	mappings := h.availableRuleMappings()
	if len(mappings) == 0 {
		h.log.Info("No NodeFeatureRule API is served by the cluster",
			"effect", "no npu.family node labels until NFD is installed; no driver pool can be built")
		return nil
	}
	for _, mapping := range mappings {
		if err := h.reconcileRule(ctx, owner, mapping); err != nil {
			// Missing RBAC for a served NFR group is environmental, exactly
			// like the missing CRD above (e.g. an OLM install whose bundle
			// CSV predates this component -- bundles regenerate at release
			// time). Degrade to this component's notReady instead of
			// aborting every remaining operand.
			if kapierrors.IsForbidden(err) {
				h.log.Info("RBAC forbids managing NodeFeatureRules",
					"group", mapping.GroupVersionKind.Group, "error", err,
					"effect", "no npu.family node labels from this API group until the operator's RBAC covers it")
				continue
			}
			return err
		}
	}
	return nil
}

func (h *npuFamilyRulePatcher) CleanUp(ctx context.Context, _ *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.V(consts.VDebug).Info("Cleaning up disabled component", "component", "NPU Family Rule")
	for _, mapping := range h.availableRuleMappings() {
		if err := h.deleteIfExists(ctx, h.emptyRule(mapping)); err != nil {
			return err
		}
	}
	return nil
}

// IsReady reports on the rule object itself — there are no pods to wait for.
// Desired counts the served NodeFeatureRule APIs and Ready the ones where the
// rule exists; zero served APIs is the actionable failure mode, because the
// labels this component exists for can then never appear.
func (h *npuFamilyRulePatcher) IsReady(ctx context.Context, _ int32) ReadinessReport {
	mappings := h.availableRuleMappings()
	if len(mappings) == 0 {
		return ReadinessReport{
			State:   rblnv1beta1.ComponentStateNotReady,
			Message: "no NodeFeatureRule API (nfd.k8s-sigs.io or nfd.openshift.io) is served; install NFD to enable NPU family labels",
		}
	}
	report := ReadinessReport{}
	for _, mapping := range mappings {
		rule := h.emptyRule(mapping)
		if err := h.client.Get(ctx, client.ObjectKeyFromObject(rule), rule); err != nil {
			return ReadinessReport{
				State:   rblnv1beta1.ComponentStateNotReady,
				Message: fmt.Sprintf("NodeFeatureRule %s (%s) not found: %v", h.name, mapping.GroupVersionKind.Group, err),
			}
		}
		if ref := foreignRuleController(rule); ref != nil {
			return ReadinessReport{
				State: rblnv1beta1.ComponentStateNotReady,
				Message: fmt.Sprintf(
					"NodeFeatureRule %s (%s) exists but is controlled by %s %q, not this operator; rename or remove it so the operator can manage NPU family labels",
					h.name, mapping.GroupVersionKind.Group, ref.Kind, ref.Name),
			}
		}
		report.Desired++
		report.Ready++
	}
	report.State = rblnv1beta1.ComponentStateReady
	return report
}

// availableRuleMappings resolves which NodeFeatureRule APIs the cluster
// serves. NoMatch means that CRD is absent; the manager's lazy RESTMapper
// re-discovers on demand, so a CRD installed later is picked up without an
// operator restart.
func (h *npuFamilyRulePatcher) availableRuleMappings() []*meta.RESTMapping {
	mappings := make([]*meta.RESTMapping, 0, len(nodeFeatureRuleGVKs))
	for _, gvk := range nodeFeatureRuleGVKs {
		mapping, err := h.client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			if !meta.IsNoMatchError(err) {
				h.log.Error(err, "Failed to resolve NodeFeatureRule mapping", "group", gvk.Group)
			}
			continue
		}
		mappings = append(mappings, mapping)
	}
	return mappings
}

// emptyRule returns the identity of this component's rule in one API group.
// The upstream kind is cluster-scoped; scope is read from the mapping so a
// namespaced variant lands in the operator namespace instead of failing.
func (h *npuFamilyRulePatcher) emptyRule(mapping *meta.RESTMapping) *unstructured.Unstructured {
	rule := &unstructured.Unstructured{}
	rule.SetGroupVersionKind(mapping.GroupVersionKind)
	rule.SetName(h.name)
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		rule.SetNamespace(h.namespace)
	}
	return rule
}

func (h *npuFamilyRulePatcher) reconcileRule(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy, mapping *meta.RESTMapping) error {
	// Probe for a rule controlled by something other than an
	// RBLNClusterPolicy: overwriting it would fight that controller forever
	// and GC-delete its object when the policy goes away. Skipping is not an
	// error -- failing here would abort every remaining operand's patch.
	probe := h.emptyRule(mapping)
	if err := h.client.Get(ctx, client.ObjectKeyFromObject(probe), probe); err == nil {
		if ref := foreignRuleController(probe); ref != nil {
			h.log.Info("NodeFeatureRule is controlled by another owner; leaving it untouched",
				"name", h.name, "group", mapping.GroupVersionKind.Group, "ownerKind", ref.Kind, "ownerName", ref.Name,
				"effect", "the operator's npu.family rules are not applied in this API group")
			return nil
		}
	} else if !kapierrors.IsNotFound(err) {
		return err
	}

	rule := h.emptyRule(mapping)
	res, err := controllerutil.CreateOrPatch(ctx, h.client, rule, func() error {
		if err := unstructured.SetNestedField(rule.Object, npuFamilyRuleSpec(), "spec"); err != nil {
			return err
		}
		adoptRule(rule, owner)
		return ctrl.SetControllerReference(owner, rule, h.scheme)
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile NodeFeatureRule", "name", h.name, "group", mapping.GroupVersionKind.Group)
		return err
	}
	h.log.Info("Reconciled NodeFeatureRule", "name", rule.GetName(), "group", mapping.GroupVersionKind.Group, "result", res)
	return nil
}

// adoptRule drops a controller reference left by a replaced RBLNClusterPolicy
// (recreated with a new UID), which SetControllerReference would otherwise
// refuse to re-own until GC catches up. Only RBLNClusterPolicy refs are
// dropped; a ref of any other kind belongs to someone else.
func adoptRule(rule *unstructured.Unstructured, owner *rblnv1beta1.RBLNClusterPolicy) {
	refs := rule.GetOwnerReferences()
	kept := make([]metav1.OwnerReference, 0, len(refs))
	for _, ref := range refs {
		if isPolicyController(ref) && ref.UID != owner.GetUID() {
			continue
		}
		kept = append(kept, ref)
	}
	rule.SetOwnerReferences(kept)
}

// foreignRuleController returns the rule's controller ownerReference when it
// belongs to something other than an RBLNClusterPolicy, nil otherwise.
func foreignRuleController(rule *unstructured.Unstructured) *metav1.OwnerReference {
	refs := rule.GetOwnerReferences()
	for i := range refs {
		ref := refs[i]
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if isPolicyController(ref) {
			continue
		}
		return &refs[i]
	}
	return nil
}

func isPolicyController(ref metav1.OwnerReference) bool {
	group := ref.APIVersion
	if i := strings.IndexByte(group, '/'); i >= 0 {
		group = group[:i]
	}
	return ref.Kind == "RBLNClusterPolicy" && group == rblnv1beta1.GroupVersion.Group
}

// npuFamilyRuleSpec renders consts.NPUFamilies as NodeFeatureRule rules in
// JSON form (SetNestedField deep-copies and rejects anything but
// map[string]any / []any / scalars).
func npuFamilyRuleSpec() map[string]any {
	rules := make([]any, 0, len(consts.NPUFamilies))
	for _, family := range consts.NPUFamilies {
		ids := make([]any, 0, len(family.DeviceIDs))
		for _, id := range family.DeviceIDs {
			ids = append(ids, id)
		}
		rules = append(rules, map[string]any{
			"name": "rbln-family-" + family.Name,
			"labels": map[string]any{
				consts.RBLNNPUFamilyLabelKey: family.Name,
			},
			"matchFeatures": []any{
				map[string]any{
					"feature": "pci.device",
					"matchExpressions": map[string]any{
						"vendor": map[string]any{"op": "In", "value": []any{consts.RBLNVendorCode}},
						"device": map[string]any{"op": "In", "value": ids},
					},
				},
			},
		})
	}
	return map[string]any{"rules": rules}
}
