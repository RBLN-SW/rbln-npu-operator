package components

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/registry"
)

// ─── Struct ──────────────────────────────────────────────────────────────────

type driverManagerPatcher struct {
	basePatcher
	desiredSpec *rebellionsaiv1alpha1.RBLNDriverSpec
	checker     ImageChecker
	// ownedNodes is the owner resolver's node snapshot for this instance
	// from the same reconcile pass. Pools are partitioned from it, never
	// from a fresh node List: an informer List could lag the resolver's
	// owner-label writes and misread a fully-owned instance as empty.
	ownedNodes []corev1.Node
	// diagnostics carries pool failures that produce no DaemonSet, and so
	// would otherwise be invisible to PoolStatuses.
	diagnostics PoolDiagnostics
}

func NewDriverManagerPatcher(
	client client.Client,
	apiReader client.Reader,
	log logr.Logger,
	namespace string,
	driver *rebellionsaiv1alpha1.RBLNDriver,
	scheme *runtime.Scheme,
	checker ImageChecker,
	openshiftVersion string,
	ownedNodes []corev1.Node,
) (DriverPatcher, error) {
	if driver == nil {
		return nil, fmt.Errorf("driver is nil")
	}
	if checker == nil {
		return nil, fmt.Errorf("checker is nil")
	}
	return &driverManagerPatcher{
		basePatcher: basePatcher{
			client:           client,
			apiReader:        apiReader,
			log:              log,
			scheme:           scheme,
			name:             driverManagerName,
			instanceName:     driver.Name,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
		},
		desiredSpec: &driver.Spec,
		checker:     checker,
		ownedNodes:  ownedNodes,
	}, nil
}

// ─── Interface methods ───────────────────────────────────────────────────────

func (h *driverManagerPatcher) Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	// Stale diagnostics from a previous reconcile must not leak into this
	// one -- a pool that recovered (image published, family label restored)
	// would otherwise keep reporting a failure that no longer holds.
	h.diagnostics = PoolDiagnostics{}

	if err := h.reconcileServiceAccount(ctx, owner); err != nil {
		return err
	}
	if err := h.reconcileOpenShiftRBAC(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRole(ctx, owner); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx, owner); err != nil {
		return err
	}
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}

	// Partition pools from the resolver's node snapshot, and select on the
	// operator-stamped owner label rather than the user selector: the
	// resolver guarantees one owner per node, which makes one-driver-per-node
	// a scheduler-level invariant even when selectors of two CRs overlap.
	nodePools, nodesWithoutFamily := buildNodePools(h.ownedNodes, map[string]string{
		consts.RBLNDriverOwnerLabelKey: h.instanceName,
	}, h.log)
	h.diagnostics.NodesWithoutFamily = nodesWithoutFamily
	if len(nodePools) == 0 {
		if len(nodesWithoutFamily) > 0 {
			// Owned nodes exist but none carry npu.family yet (NFD lag, or
			// an unrecognized device ID) — not "this driver owns nothing".
			// Tearing down here would uninstall the driver fleet-wide on
			// every label gap. Debug, not Info: the CR condition is the
			// user-facing signal.
			h.log.V(consts.VDebug).Info("Owned nodes lack the npu.family label; keeping existing DaemonSets",
				"driver", h.instanceName, "nodesWithoutFamily", len(nodesWithoutFamily))
			return nil
		}
		h.log.Info("The resolver assigned this driver no nodes; removing its DaemonSets",
			"driver", h.instanceName, "ownerLabel", consts.RBLNDriverOwnerLabelKey)
		stale, err := h.findStaleDaemonSets(ctx, nil)
		if err != nil {
			return err
		}
		return h.reapDaemonSets(ctx, stale)
	}

	pullSecrets := h.fetchPullSecrets(ctx)

	desiredPoolNames := make(map[string]struct{}, len(nodePools))
	for _, pool := range nodePools {
		desiredPoolNames[pool.name] = struct{}{}
	}

	renderable := make([]poolCheck, 0, len(nodePools))
	for _, pool := range nodePools {
		imagePath, err := h.desiredSpec.GetPrecompiledImagePath(pool.getOS(), pool.kernel, pool.family)
		if err != nil {
			return err
		}
		verdict, cause := h.checker.Check(ctx, imagePath, pullSecrets)
		if verdict == registry.VerdictNotFound {
			// Render nothing for this pool: a 404 means the tag is absent, and
			// creating the DaemonSet would only queue an ImagePullBackOff. A
			// pull secret the operator could not read does not change this --
			// a referenced-but-missing secret fails kubelet's pull too, so
			// blocking is the same verdict either way; the condition message
			// names the unread secrets so a registry that answers 404 to
			// unauthorized reads is not mistaken for a missing image. Debug,
			// not Info: this repeats every pass until the image is published;
			// the CR condition is the user-facing signal. Any existing
			// DaemonSet is left alone -- OnDelete means a running driver pod is
			// never replaced by this update anyway.
			h.diagnostics.MissingImagePools = append(h.diagnostics.MissingImagePools,
				MissingImagePool{Pool: pool.name, Image: imagePath})
			h.log.V(consts.VDebug).Info("Driver image not found in registry; skipping pool",
				"pool", pool.name, "image", imagePath, "err", cause)
			continue
		}
		renderable = append(renderable, poolCheck{pool: pool, imagePath: imagePath})
	}

	// Reap before creating, so a pool rename never leaves an old and a new
	// DaemonSet targeting the same node in a create-then-clean window: a brief
	// gap with no driver pod is fine (deleting a pod doesn't unload the kernel
	// module), two concurrent installers on one host is not.
	//
	// Held back while any pool is unrenderable -- a node with no family label,
	// or a pool whose image is missing. A stale DaemonSet may be the only driver
	// those nodes have, and reaping it would be teardown with no replacement.
	// Holding costs nothing (a stale pool has no matching nodes, so its
	// DaemonSet runs zero pods) but the kept DaemonSet may still schedule onto a
	// renderable pool's nodes, so new pool DaemonSets wait too. Both resume on
	// the pass after the gap heals.
	stale, err := h.findStaleDaemonSets(ctx, desiredPoolNames)
	if err != nil {
		return err
	}
	unrenderablePools := len(h.diagnostics.MissingImagePools) > 0 || len(nodesWithoutFamily) > 0
	keptStaleDaemonSets := len(stale) > 0 && unrenderablePools
	if !keptStaleDaemonSets {
		if err := h.reapDaemonSets(ctx, stale); err != nil {
			return err
		}
	}

	for _, chk := range renderable {
		if err := h.handleDaemonSet(ctx, owner, chk.pool, chk.imagePath, keptStaleDaemonSets); err != nil {
			return err
		}
	}

	return nil
}

// poolCheck is one node pool that survived the image check, paired with the
// image path the check ran against -- the same string the DaemonSet deploys.
type poolCheck struct {
	pool      nodePool
	imagePath string
}

// fetchPullSecrets resolves the spec's pull secrets once per Patch call.
// apiReader, not the cached client: a cached Get would lazily start a
// cluster-wide Secrets informer, and this component only holds namespaced get --
// without list/watch RBAC the cache sync blocks forever.
//
// A Get failure is never fatal -- an anonymous check is the same posture as the
// pull kubelet will attempt anyway -- but it is recorded, because an anonymous
// check is the one way a NotFound verdict can be wrong (a registry that answers
// 404 rather than 401/403 to unauthorized reads). Every failure kind counts, not
// just NotFound: Forbidden is the likelier one, and it is exactly what an OLM
// upgrade whose CSV predates the secret-reader Role produces.
func (h *driverManagerPatcher) fetchPullSecrets(ctx context.Context) []corev1.Secret {
	secrets := make([]corev1.Secret, 0, len(h.desiredSpec.ImagePullSecrets))
	for _, name := range h.desiredSpec.ImagePullSecrets {
		secret := &corev1.Secret{}
		if err := h.apiReader.Get(ctx, client.ObjectKey{Name: name, Namespace: h.namespace}, secret); err != nil {
			h.diagnostics.UnreadablePullSecrets = append(h.diagnostics.UnreadablePullSecrets,
				fmt.Sprintf("%s (%s)", name, kapierrors.ReasonForError(err)))
			h.log.Info("Configured image pull secret is unreadable",
				"name", name, "namespace", h.namespace, "err", err,
				"effect", "registry image check runs anonymously; a private image may be misread as missing and block its pool")
			continue
		}
		secrets = append(secrets, *secret)
	}
	return secrets
}

// Diagnostics returns a copy of the pool failures from the last Patch call,
// so callers cannot mutate this patcher's state. MissingImagePools is sorted
// because Patch fills it in map-derived order: unsorted, the rendered
// condition message flaps between identical states over an unchanged fleet,
// and every flap costs a status write and a requeue.
func (h *driverManagerPatcher) Diagnostics() PoolDiagnostics {
	missing := append([]MissingImagePool(nil), h.diagnostics.MissingImagePools...)
	sort.Slice(missing, func(i, j int) bool { return missing[i].Pool < missing[j].Pool })
	return PoolDiagnostics{
		NodesWithoutFamily:    append([]string(nil), h.diagnostics.NodesWithoutFamily...),
		MissingImagePools:     missing,
		UnreadablePullSecrets: append([]string(nil), h.diagnostics.UnreadablePullSecrets...),
	}
}

// PoolStatuses returns an empty slice when no node is a candidate yet, which
// callers read as "nothing to do" rather than a failure. Uses apiReader so a
// Delete issued earlier in this same reconcile is observed here.
func (h *driverManagerPatcher) PoolStatuses(ctx context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, error) {
	dsList := &appsv1.DaemonSetList{}
	if err := h.apiReader.List(ctx, dsList,
		client.InNamespace(h.namespace),
		client.MatchingLabels(map[string]string{
			driverManagerAppLabelKey:      h.name,
			driverManagerInstanceLabelKey: h.instanceName,
		}),
	); err != nil {
		return nil, fmt.Errorf("list driver DaemonSets: %w", err)
	}

	pools := make([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, 0, len(dsList.Items))
	for i := range dsList.Items {
		ds := &dsList.Items[i]
		desired := ds.Status.DesiredNumberScheduled
		ready := ds.Status.NumberReady

		state := rebellionsaiv1alpha1.DriverPoolStateReady
		if desired == 0 || ready != desired || ds.Status.NumberUnavailable > 0 {
			state = rebellionsaiv1alpha1.DriverPoolStateProgressing
		}

		pools = append(pools, rebellionsaiv1alpha1.RBLNDriverPoolStatus{
			Name:    ds.Name,
			Desired: desired,
			Ready:   ready,
			State:   state,
		})
	}
	return pools, nil
}
