package upgrade

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

type NodeUpgradeStateProvider struct {
	K8sClient     client.Client
	nodeMutex     KeyedMutex
	eventRecorder record.EventRecorder
}

func NewNodeUpgradeStateProvider(c client.Client, r record.EventRecorder) *NodeUpgradeStateProvider {
	return &NodeUpgradeStateProvider{K8sClient: c, eventRecorder: r}
}

func (p *NodeUpgradeStateProvider) GetNode(ctx context.Context, name string) (*corev1.Node, error) {
	n := &corev1.Node{}
	err := p.K8sClient.Get(ctx, types.NamespacedName{Name: name}, n)
	return n, err
}

// All state changes use a fresh, versioned observation. In-flight work may
// finish after teardown, so guarded changes are conditional on the live state.
func (p *NodeUpgradeStateProvider) mutate(ctx context.Context, node *corev1.Node, expected *string, edit func(*corev1.Node) bool) error {
	unlock := p.nodeMutex.Lock(node.Name)
	defer unlock()
	return retryNodeConflicts(ctx, func() error {
		live, err := p.GetNode(ctx, node.Name)
		if err != nil {
			return err
		}
		old := live.Labels[UpgradeStateLabelKey]
		if expected != nil && old != *expected {
			log.FromContext(ctx).V(consts.VDebug).Info("Discarding a stale node operation",
				"node", node.Name, "expectedState", logKeyForNodeState(*expected), "state", logKeyForNodeState(old))
			return nil
		}
		before := live.DeepCopy()
		if !edit(live) {
			return nil
		}
		if err := p.K8sClient.Patch(ctx, live, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		node.Labels = live.Labels
		node.Annotations = live.Annotations
		node.ResourceVersion = live.ResourceVersion
		if next := live.Labels[UpgradeStateLabelKey]; next != old {
			log.FromContext(ctx).Info("Node upgrade state committed", "node", node.Name,
				"previousState", logKeyForNodeState(old), "state", logKeyForNodeState(next))
		}
		p.recordStateTransitionEvent(live, old, live.Labels[UpgradeStateLabelKey])
		return nil
	})
}

// Only conflicts justify repeating a read-modify-write. In particular, return
// cancellation and deadline failures without converting them to a success.
func retryNodeConflicts(ctx context.Context, run func() error) error {
	// Node resource versions also move when kubelet and discovery controllers
	// update unrelated fields. Spread retries across that contention window.
	backoff := retry.DefaultBackoff
	attempts := backoff.Steps
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = run()
		if !apierrors.IsConflict(err) || attempt+1 == attempts {
			return err
		}
		timer := time.NewTimer(backoff.Step())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (p *NodeUpgradeStateProvider) ChangeNodeUpgradeState(ctx context.Context, n *corev1.Node, to string) error {
	return p.changeNodeUpgradeState(ctx, n, to, "", false)
}

func (p *NodeUpgradeStateProvider) ChangeNodeUpgradeStateFrom(ctx context.Context, n *corev1.Node, from, to string) error {
	return p.changeNodeUpgradeState(ctx, n, to, from, true)
}

func (p *NodeUpgradeStateProvider) changeNodeUpgradeState(ctx context.Context, n *corev1.Node, to, from string, guarded bool) error {
	var expected *string
	if guarded {
		expected = &from
	}
	return p.mutate(ctx, n, expected, func(live *corev1.Node) bool {
		if live.Labels[UpgradeStateLabelKey] == to {
			return false
		}
		if live.Labels == nil {
			live.Labels = map[string]string{}
		}
		live.Labels[UpgradeStateLabelKey] = to
		return true
	})
}

func (p *NodeUpgradeStateProvider) recordStateTransitionEvent(n *corev1.Node, from, to string) {
	if from == to {
		return
	}
	kind, reason, message := "", "", ""
	switch {
	case from == UpgradeStateUpgradeRequired && to == UpgradeStateCordonRequired:
		kind, reason, message = corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeStarted, "Driver upgrade started; node will be cordoned"
	case to == UpgradeStateFailed:
		kind, reason = corev1.EventTypeWarning, consts.RBLNEventReasonDriverUpgradeFailed
		message = fmt.Sprintf("Driver upgrade failed at state %q", logKeyForNodeState(from))
		if diagnosis := n.Annotations[UpgradeFailureReasonAnnotationKey]; diagnosis != "" {
			message += ": " + diagnosis
		}
	case to == UpgradeStateSkipped:
		kind, reason = corev1.EventTypeWarning, consts.RBLNEventReasonDriverUpgradeSkipped
		message = fmt.Sprintf("Driver upgrade skipped at state %q; node returns to service on the old driver", logKeyForNodeState(from))
		if diagnosis := n.Annotations[UpgradeSkipReasonAnnotationKey]; diagnosis != "" {
			message += ": " + diagnosis
		}
	case to == UpgradeStateDone && IsInProgressUpgradeState(from):
		kind, reason, message = corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeCompleted, "Driver upgrade completed; node is schedulable"
	}
	if reason != "" {
		recordNodeEvent(p.eventRecorder, n, kind, reason, message)
	}
}

func (p *NodeUpgradeStateProvider) SetNodeUpgradeAnnotation(ctx context.Context, n *corev1.Node, key, value string) error {
	return p.SetNodeUpgradeAnnotations(ctx, n, map[string]any{key: value})
}

func (p *NodeUpgradeStateProvider) RemoveNodeUpgradeAnnotation(ctx context.Context, n *corev1.Node, key string) error {
	return p.SetNodeUpgradeAnnotations(ctx, n, map[string]any{key: nil})
}

func (p *NodeUpgradeStateProvider) SetNodeUpgradeAnnotations(ctx context.Context, n *corev1.Node, values map[string]any) error {
	return p.mutate(ctx, n, nil, func(live *corev1.Node) bool { return editAnnotations(live, values) })
}

func editAnnotations(n *corev1.Node, values map[string]any) bool {
	changed := false
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	for key, value := range values {
		old, exists := n.Annotations[key]
		if value == nil {
			if exists {
				delete(n.Annotations, key)
				changed = true
			}
		} else if v, ok := value.(string); ok && (!exists || old != v) {
			n.Annotations[key] = v
			changed = true
		}
	}
	return changed
}

func truncateReason(s string) string {
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}

func markNodeUpgradeFailed(ctx context.Context, p *NodeUpgradeStateProvider, n *corev1.Node, step, reason string) error {
	return p.mutate(ctx, n, &step, func(live *corev1.Node) bool {
		editAnnotations(live, map[string]any{UpgradeFailureStepAnnotationKey: step, UpgradeFailureReasonAnnotationKey: truncateReason(reason)})
		if live.Labels == nil {
			live.Labels = map[string]string{}
		}
		live.Labels[UpgradeStateLabelKey] = UpgradeStateFailed
		return true
	})
}

func markNodeUpgradeSkipped(ctx context.Context, p *NodeUpgradeStateProvider, n *corev1.Node, from, reason string) error {
	return p.mutate(ctx, n, &from, func(live *corev1.Node) bool {
		editAnnotations(live, map[string]any{UpgradeSkipReasonAnnotationKey: truncateReason(strings.TrimSpace(reason))})
		if live.Labels == nil {
			live.Labels = map[string]string{}
		}
		live.Labels[UpgradeStateLabelKey] = UpgradeStateSkipped
		return true
	})
}
