/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestNodeUpgradeStateProviderChangeState(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	err := provider.ChangeNodeUpgradeState(ctx, node, UpgradeStateCordonRequired)
	if err != nil {
		t.Fatalf("ChangeNodeUpgradeState() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateCordonRequired {
		t.Fatalf("label = %q, want %q", got, UpgradeStateCordonRequired)
	}
}

func TestNodeConflictRetriesAreBoundedAndCancelable(t *testing.T) {
	conflict := apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "worker", errors.New("concurrent update"))
	t.Run("contention has a bounded retry budget", func(t *testing.T) {
		calls := 0
		err := retryNodeConflicts(context.Background(), func() error {
			calls++
			return conflict
		})
		if !apierrors.IsConflict(err) || calls != retry.DefaultBackoff.Steps {
			t.Fatalf("calls=%d, err=%v", calls, err)
		}
	})
	t.Run("an unrelated update can settle before the final attempt", func(t *testing.T) {
		calls := 0
		err := retryNodeConflicts(context.Background(), func() error {
			calls++
			if calls < retry.DefaultBackoff.Steps {
				return conflict
			}
			return nil
		})
		if err != nil || calls != retry.DefaultBackoff.Steps {
			t.Fatalf("calls=%d, err=%v", calls, err)
		}
	})
	t.Run("cancellation interrupts the wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		err := retryNodeConflicts(ctx, func() error {
			calls++
			cancel()
			return conflict
		})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("calls=%d, err=%v", calls, err)
		}
	})
	t.Run("other failures are not retried", func(t *testing.T) {
		calls := 0
		failure := errors.New("unavailable")
		err := retryNodeConflicts(context.Background(), func() error { calls++; return failure })
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("calls=%d, err=%v", calls, err)
		}
	})
}

func TestNodeStateConflictRechecksLiveState(t *testing.T) {
	for _, changedState := range []bool{false, true} {
		name := "preserves concurrent metadata and retries"
		if changedState {
			name = "discards a verdict after the phase changes"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			patches := 0
			provider, recorder := newEventTestProvider(t, interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					if patches == 1 {
						live := &corev1.Node{}
						if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
							return err
						}
						live.Annotations = map[string]string{"test.example.com/concurrent": "preserved"}
						if changedState {
							live.Labels[UpgradeStateLabelKey] = UpgradeStateDone
						}
						if err := c.Update(ctx, live); err != nil {
							return err
						}
					}
					// Exercise the patch's resourceVersion precondition, rather
					// than returning an artificial Conflict from the interceptor.
					err := c.Patch(ctx, obj, patch, opts...)
					if patches == 1 && !apierrors.IsConflict(err) {
						t.Errorf("stale patch should conflict, got %v", err)
					}
					return err
				},
			})
			node := createStateNode(t, provider, UpgradeStateUpgradeRequired)
			if err := provider.ChangeNodeUpgradeStateFrom(ctx, node, UpgradeStateUpgradeRequired, UpgradeStateCordonRequired); err != nil {
				t.Fatal(err)
			}
			live, err := provider.GetNode(ctx, node.Name)
			if err != nil {
				t.Fatal(err)
			}
			if live.Annotations["test.example.com/concurrent"] != "preserved" {
				t.Fatal("retry overwrote concurrent metadata")
			}
			if changedState {
				if patches != 1 || live.Labels[UpgradeStateLabelKey] != UpgradeStateDone {
					t.Fatal("retry did not recheck the phase before writing")
				}
				expectNoEvent(t, recorder)
				return
			}
			if patches != 2 || live.Labels[UpgradeStateLabelKey] != UpgradeStateCordonRequired ||
				node.Labels[UpgradeStateLabelKey] != UpgradeStateCordonRequired {
				t.Fatal("retry did not commit and publish the transition")
			}
			expectEvent(t, recorder, corev1.EventTypeNormal, consts.RBLNEventReasonDriverUpgradeStarted, "Driver upgrade started")
			expectNoEvent(t, recorder)
		})
	}
}

func TestNodeUpgradeStateProviderSetAndRemoveAnnotation(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	// Set annotation
	err := provider.SetNodeUpgradeAnnotation(ctx, node, "test-key", "test-value")
	if err != nil {
		t.Fatalf("SetNodeUpgradeAnnotation() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Annotations["test-key"]; got != "test-value" {
		t.Fatalf("annotation = %q, want %q", got, "test-value")
	}

	// Remove annotation
	err = provider.RemoveNodeUpgradeAnnotation(ctx, &updated, "test-key")
	if err != nil {
		t.Fatalf("RemoveNodeUpgradeAnnotation() error: %v", err)
	}

	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, exists := updated.Annotations["test-key"]; exists {
		t.Fatal("annotation should have been removed")
	}
}

func TestNodeUpgradeStateProviderSetAnnotationsBatch(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        "worker-1",
		Annotations: map[string]string{"stale-key": "stale-value"},
	}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	err := provider.SetNodeUpgradeAnnotations(ctx, node, map[string]any{
		"key-a":     "value-a",
		"key-b":     "value-b",
		"stale-key": nil,
	})
	if err != nil {
		t.Fatalf("SetNodeUpgradeAnnotations() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Annotations["key-a"]; got != "value-a" {
		t.Fatalf("annotation key-a = %q, want %q", got, "value-a")
	}
	if got := updated.Annotations["key-b"]; got != "value-b" {
		t.Fatalf("annotation key-b = %q, want %q", got, "value-b")
	}
	if _, exists := updated.Annotations["stale-key"]; exists {
		t.Fatal("nil-valued annotation should have been removed")
	}
}

func TestTruncateReason(t *testing.T) {
	if got := truncateReason("short reason"); got != "short reason" {
		t.Fatalf("truncateReason = %q, want unchanged", got)
	}

	long := strings.Repeat("x", 500)
	got := truncateReason(long)
	if len(got) != 403 || !strings.HasSuffix(got, "...") {
		t.Fatalf("truncateReason len = %d, want 403 ending in ellipsis", len(got))
	}
}

func TestNodeUpgradeStateProviderGetNode(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	t.Run("returns existing node", func(t *testing.T) {
		got, err := provider.GetNode(ctx, "worker-1")
		if err != nil {
			t.Fatalf("GetNode() error: %v", err)
		}
		if got.Name != "worker-1" {
			t.Fatalf("node.Name = %q, want worker-1", got.Name)
		}
	})

	t.Run("returns error for nonexistent node", func(t *testing.T) {
		_, err := provider.GetNode(ctx, "nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent node")
		}
	})
}

func TestChangeNodeUpgradeStateFrom(t *testing.T) {
	tests := map[string]struct {
		current   string
		from      string
		wantState string
	}{
		"moves when the node is still in the expected state": {
			current: UpgradeStatePodDeletionRequired, from: UpgradeStatePodDeletionRequired, wantState: UpgradeStatePodRestartRequired,
		},
		"refuses when the node moved on": {
			current: UpgradeStateDone, from: UpgradeStatePodDeletionRequired, wantState: UpgradeStateDone,
		},
		"refuses when the label is gone": {
			current: "", from: UpgradeStatePodDeletionRequired, wantState: "",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			labels := map[string]string{}
			if tc.current != "" {
				labels[UpgradeStateLabelKey] = tc.current
			}
			p := newTestNodeUpgradeStateProvider(t)
			k8sClient := p.K8sClient
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: labels}}
			if err := k8sClient.Create(context.Background(), node); err != nil {
				t.Fatalf("create node: %v", err)
			}

			if err := p.ChangeNodeUpgradeStateFrom(context.Background(), node.DeepCopy(), tc.from, UpgradeStatePodRestartRequired); err != nil {
				t.Fatalf("ChangeNodeUpgradeStateFrom: %v", err)
			}
			updated := &corev1.Node{}
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "n"}, updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("state = %q, want %q", got, tc.wantState)
			}
		})
	}
}
