package k8sutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestNodeLabelsChangedExceptPredicate(t *testing.T) {
	p := NodeLabelsChangedExceptPredicate("rebellions.ai/npu.driver.owner")

	node := func(labels map[string]string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: labels}}
	}

	if !p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"a": "1"}),
		ObjectNew: node(map[string]string{"a": "2"}),
	}) {
		t.Error("label value change must trigger")
	}
	if !p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"a": "1"}),
		ObjectNew: node(map[string]string{"a": "1", "b": "2"}),
	}) {
		t.Error("label addition must trigger")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"a": "1"}),
		ObjectNew: node(map[string]string{"a": "1"}),
	}) {
		t.Error("no label change must not trigger")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"a": "1"}),
		ObjectNew: node(map[string]string{"a": "1", "rebellions.ai/npu.driver.owner": "rbln-driver"}),
	}) {
		t.Error("owner-label-only change must not trigger (operator's own write)")
	}
	if !p.Create(event.CreateEvent{Object: node(nil)}) {
		t.Error("create must trigger")
	}
	if !p.Delete(event.DeleteEvent{Object: node(nil)}) {
		t.Error("delete must trigger")
	}
}

func TestDeletionTimestampChangedPredicate(t *testing.T) {
	p := DeletionTimestampChangedPredicate()

	obj := func(dt *metav1.Time) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", DeletionTimestamp: dt}}
	}
	now := metav1.Now()

	if !p.Update(event.UpdateEvent{ObjectOld: obj(nil), ObjectNew: obj(&now)}) {
		t.Error("deletionTimestamp being set must trigger")
	}
	if p.Update(event.UpdateEvent{ObjectOld: obj(nil), ObjectNew: obj(nil)}) {
		t.Error("update without deletionTimestamp change must not trigger")
	}
	if p.Update(event.UpdateEvent{ObjectOld: obj(&now), ObjectNew: obj(&now)}) {
		t.Error("update of an already-deleting object must not trigger")
	}
	if !p.Create(event.CreateEvent{Object: obj(nil)}) {
		t.Error("create must trigger")
	}
	if !p.Delete(event.DeleteEvent{Object: obj(nil)}) {
		t.Error("delete must trigger")
	}
}

func TestNodeLabelPrefixChangedPredicateExceptKeys(t *testing.T) {
	p := NodeLabelPrefixChangedPredicate("rebellions.ai/", "rebellions.ai/npu.driver.owner")

	node := func(labels map[string]string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: labels}}
	}

	if !p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"rebellions.ai/npu.present": "true"}),
		ObjectNew: node(map[string]string{"rebellions.ai/npu.present": "false"}),
	}) {
		t.Error("prefix-key change must trigger")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"rebellions.ai/npu.present": "true"}),
		ObjectNew: node(map[string]string{
			"rebellions.ai/npu.present":      "true",
			"rebellions.ai/npu.driver.owner": "rbln-driver",
		}),
	}) {
		t.Error("owner-key-only change must not trigger")
	}
	if p.Update(event.UpdateEvent{
		ObjectOld: node(map[string]string{"a": "1"}),
		ObjectNew: node(map[string]string{"a": "2"}),
	}) {
		t.Error("non-prefix change must not trigger")
	}
}
