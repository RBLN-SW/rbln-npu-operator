package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// reservedSelectorKeys cannot appear in spec.nodeSelector: the owner key is
// operator-managed routing state, and the deploy key is the ClusterPolicy's
// node gate that the resolver already scopes on.
var reservedSelectorKeys = []string{
	consts.RBLNDriverOwnerLabelKey,
	consts.RBLNDeployDriverLabelKey,
}

// hasReservedSelectorKey returns the first reserved key present in selector.
func hasReservedSelectorKey(selector map[string]string) (string, bool) {
	for _, key := range reservedSelectorKeys {
		if _, ok := selector[key]; ok {
			return key, true
		}
	}
	return "", false
}

// ValidateDriverSpec rejects specs the resolver refuses to route: reserved
// selector keys and names unusable as label values (the CR name is stamped on
// nodes, which caps it at 63 characters). The resolver's candidate filter
// calls this function, so the two rejection sets cannot drift.
func ValidateDriverSpec(instance *rebellionsaiv1alpha1.RBLNDriver) error {
	if key, reserved := hasReservedSelectorKey(instance.Spec.NodeSelector); reserved {
		return fmt.Errorf("nodeSelector must not use reserved label key %q (the operator manages it itself)", key)
	}
	if len(instance.Name) > validation.LabelValueMaxLength {
		return fmt.Errorf("metadata.name %q exceeds %d characters and cannot be used as the %s label value",
			instance.Name, validation.LabelValueMaxLength, consts.RBLNDriverOwnerLabelKey)
	}
	return nil
}

// desiredOwnerForNode picks the single winning driver for a node: the unique
// maximal matching selector under strict-superset comparison, which makes the
// empty selector a natural fallback needing no "default" flag. An
// incomparable tie keeps the current owner if it still matches and otherwise
// leaves the node unowned, returning the tied names sorted; owner != "" with
// a non-empty tie list means sticky retention. Callers must pass only
// routable candidates.
func desiredOwnerForNode(
	nodeLabels map[string]string,
	currentOwner string,
	drivers []rebellionsaiv1alpha1.RBLNDriver,
) (string, []string) {
	matching := make([]rebellionsaiv1alpha1.RBLNDriver, 0, len(drivers))
	for _, d := range drivers {
		if selectorMatches(nodeLabels, d.Spec.NodeSelector) {
			matching = append(matching, d)
		}
	}
	if len(matching) == 0 {
		return "", nil
	}
	if len(matching) == 1 {
		return matching[0].Name, nil
	}

	maximal := make([]rebellionsaiv1alpha1.RBLNDriver, 0, len(matching))
	for i := range matching {
		subsumed := false
		for j := range matching {
			if i == j {
				continue
			}
			if selectorStrictSuperset(matching[j].Spec.NodeSelector, matching[i].Spec.NodeSelector) {
				subsumed = true
				break
			}
		}
		if !subsumed {
			maximal = append(maximal, matching[i])
		}
	}
	if len(maximal) == 1 {
		return maximal[0].Name, nil
	}

	names := make([]string, 0, len(maximal))
	for _, d := range maximal {
		names = append(names, d.Name)
	}
	sort.Strings(names)

	for _, d := range matching {
		if d.Name == currentOwner {
			return currentOwner, names
		}
	}
	return "", names
}

// selectorMatches reports whether every selector pair is present on the node.
// An empty selector matches every node in the resolver's domain.
func selectorMatches(nodeLabels, selector map[string]string) bool {
	for key, value := range selector {
		got, ok := nodeLabels[key]
		if !ok || got != value {
			return false
		}
	}
	return true
}

// selectorStrictSuperset reports whether a ⊋ b as key-value sets.
func selectorStrictSuperset(a, b map[string]string) bool {
	if len(a) <= len(b) {
		return false
	}
	for key, value := range b {
		got, ok := a[key]
		if !ok || got != value {
			return false
		}
	}
	return true
}

// OwnerChange is one node's owner-label write from a resolve pass.
type OwnerChange struct {
	// Owner is the value written; "" means the label was removed.
	Owner string
	// Node is the patched object from this pass's snapshot. Event recorders
	// must reference it, not a name-only stub: kubectl describe correlates
	// events by involvedObject.uid, which a stub leaves empty.
	Node *corev1.Node
}

// ResolveResult is the outcome of one global owner-assignment pass.
type ResolveResult struct {
	// OwnedNodes maps each routable RBLNDriver name to the nodes routed to
	// it, from this pass's snapshot. Consumers partition node pools from
	// these objects instead of re-listing nodes, so the pool view can never
	// lag the resolver's owner-label writes. Every routable candidate has an
	// entry, so a selector matching nothing reads as an empty list.
	OwnedNodes map[string][]corev1.Node
	// UncoveredNodes are nodes with no owner after this pass — no selector
	// matches, or an unresolved selector tie left them unowned; sorted.
	UncoveredNodes []string
	// ConflictNodes maps RBLNDriver name to nodes where it ties with another
	// driver. Node names are ascending and stable across passes, so consumers
	// can embed them in status messages.
	ConflictNodes map[string][]string
	// OwnerChanges maps nodes patched this pass to the change written.
	// Delivery is at-least-once, not exactly-on-transition: a stale cache can
	// repeat an already-applied change next pass.
	OwnerChanges map[string]OwnerChange
	// ScopeExitNodes are nodes that left the resolver's domain and had a
	// stale owner label removed, sorted. Kept separate from OwnerChanges:
	// leaving the domain is normal lifecycle, not a misconfiguration.
	ScopeExitNodes []string
}

type OwnerResolver struct {
	client client.Client
	log    logr.Logger
}

func NewOwnerResolver(c client.Client, log logr.Logger) *OwnerResolver {
	return &OwnerResolver{client: c, log: log}
}

// Resolve computes the desired owner for every driver-deploy node and patches
// only the nodes whose owner label differs. All desired owners are computed
// from one node snapshot before any write, so the decision is globally
// consistent; application across nodes is still partial if a patch fails
// mid-pass, and the next pass converges.
func (r *OwnerResolver) Resolve(ctx context.Context) (*ResolveResult, error) {
	driverList := &rebellionsaiv1alpha1.RBLNDriverList{}
	if err := r.client.List(ctx, driverList); err != nil {
		return nil, fmt.Errorf("list RBLNDrivers for owner resolution: %w", err)
	}

	candidates := make([]rebellionsaiv1alpha1.RBLNDriver, 0, len(driverList.Items))
	for _, d := range driverList.Items {
		if d.GetDeletionTimestamp() != nil {
			continue
		}
		// An invalid CR must fail only itself, not wedge the global
		// assignment pass.
		if err := ValidateDriverSpec(&d); err != nil {
			r.log.V(consts.VDebug).Info("Excluding unroutable RBLNDriver from routing",
				"driver", d.Name, "err", err)
			continue
		}
		candidates = append(candidates, d)
	}

	nodeList := &corev1.NodeList{}
	if err := r.client.List(ctx, nodeList, client.MatchingLabels(map[string]string{
		consts.RBLNDeployDriverLabelKey: "true",
	})); err != nil {
		return nil, fmt.Errorf("list driver-deploy nodes for owner resolution: %w", err)
	}
	// Cache List order is randomized per call; a fixed order here makes
	// ConflictNodes/patch order/log messages stable across passes instead of
	// churning on every reconcile.
	sort.Slice(nodeList.Items, func(i, j int) bool {
		return nodeList.Items[i].Name < nodeList.Items[j].Name
	})

	result := &ResolveResult{
		OwnedNodes:    map[string][]corev1.Node{},
		ConflictNodes: map[string][]string{},
		OwnerChanges:  map[string]OwnerChange{},
	}
	// Seed an entry for every candidate so a selector that matches nothing is
	// visible as driver_owned_nodes == 0 rather than a missing series.
	for _, d := range candidates {
		result.OwnedNodes[d.Name] = nil
	}

	desired := make(map[string]string, len(nodeList.Items))
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		current := node.Labels[consts.RBLNDriverOwnerLabelKey]
		owner, conflicts := desiredOwnerForNode(node.Labels, current, candidates)
		desired[node.Name] = owner
		if owner != "" {
			// Copied before the patch loop below mutates the list item, so
			// consumers see the snapshot the routing decision was made on.
			result.OwnedNodes[owner] = append(result.OwnedNodes[owner], *node)
		} else {
			result.UncoveredNodes = append(result.UncoveredNodes, node.Name)
		}
		for _, name := range conflicts {
			result.ConflictNodes[name] = append(result.ConflictNodes[name], node.Name)
		}
	}
	sort.Strings(result.UncoveredNodes)

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		current := node.Labels[consts.RBLNDriverOwnerLabelKey]
		want := desired[node.Name]
		if current == want {
			continue
		}
		var patchValue *string
		if want != "" {
			patchValue = &want
		}
		patch, err := ownerLabelPatch(patchValue)
		if err != nil {
			return nil, fmt.Errorf("build owner label patch for node %s: %w", node.Name, err)
		}
		if err := r.client.Patch(ctx, node, client.RawPatch(types.MergePatchType, patch)); err != nil {
			// The node can be gone by the time we patch it (e.g. autoscaler
			// scale-down between List and Patch); that is not a pass failure.
			if kapierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("patch driver owner label on node %s: %w", node.Name, err)
		}
		result.OwnerChanges[node.Name] = OwnerChange{Owner: want, Node: node}
	}

	// A node can leave the resolver's scope (deploy label flipped to
	// pre-installed or removed) while still carrying an owner label; clean
	// those up so DaemonSets stop selecting them.
	labeled := &corev1.NodeList{}
	if err := r.client.List(ctx, labeled, client.HasLabels{consts.RBLNDriverOwnerLabelKey}); err != nil {
		return nil, fmt.Errorf("list owner-labeled nodes for scope cleanup: %w", err)
	}
	for i := range labeled.Items {
		node := &labeled.Items[i]
		if _, inScope := desired[node.Name]; inScope {
			continue
		}
		patch, err := ownerLabelPatch(nil)
		if err != nil {
			return nil, fmt.Errorf("build owner label removal patch for node %s: %w", node.Name, err)
		}
		if err := r.client.Patch(ctx, node, client.RawPatch(types.MergePatchType, patch)); err != nil {
			if kapierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("remove stale driver owner label on node %s: %w", node.Name, err)
		}
		result.ScopeExitNodes = append(result.ScopeExitNodes, node.Name)
	}
	sort.Strings(result.ScopeExitNodes)

	if len(result.OwnerChanges) > 0 || len(result.ScopeExitNodes) > 0 {
		r.log.Info("Driver owner labels updated",
			"changed", len(result.OwnerChanges), "scopeExits", len(result.ScopeExitNodes))
	}

	return result, nil
}

// ownerLabelPatch renders a single-key merge patch for the owner label; a nil
// value removes the label.
func ownerLabelPatch(value *string) ([]byte, error) {
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]*string{consts.RBLNDriverOwnerLabelKey: value},
		},
	}
	return json.Marshal(patch)
}
