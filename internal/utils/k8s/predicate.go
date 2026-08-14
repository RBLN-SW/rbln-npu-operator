package k8sutil

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// NodeLabelKeyPredicate returns a predicate that triggers on create/delete
// only when the object carries at least one of the specified label keys, and
// on update only when one of those keys changes value.
func NodeLabelKeyPredicate(keys ...string) predicate.Funcs {
	hasAnyKey := func(labels map[string]string) bool {
		for _, key := range keys {
			if _, ok := labels[key]; ok {
				return true
			}
		}
		return false
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return hasAnyKey(e.Object.GetLabels()) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return hasAnyKey(e.Object.GetLabels()) },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldLabels := e.ObjectOld.GetLabels()
			newLabels := e.ObjectNew.GetLabels()
			for _, key := range keys {
				if oldLabels[key] != newLabels[key] {
					return true
				}
			}
			return false
		},
	}
}

// NodeLabelsChangedExceptPredicate triggers on create/delete and on any label
// change except the given keys — inverse key semantics to
// NodeLabelKeyPredicate. Use it when the labels of interest are open-ended
// and the controller must ignore keys it writes itself.
func NodeLabelsChangedExceptPredicate(exceptKeys ...string) predicate.Funcs {
	stripped := func(labels map[string]string) map[string]string {
		out := make(map[string]string, len(labels))
		for k, v := range labels {
			out[k] = v
		}
		for _, k := range exceptKeys {
			delete(out, k)
		}
		return out
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return true },
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return !reflect.DeepEqual(stripped(e.ObjectOld.GetLabels()), stripped(e.ObjectNew.GetLabels()))
		},
	}
}

// DeletionTimestampChangedPredicate triggers on create/delete and on update
// only when the object's deletionTimestamp flips between unset and set.
// Pair it (via predicate.Or) with GenerationChangedPredicate on fan-out
// watches: a finalizer-bearing delete arrives as an update that sets
// deletionTimestamp without bumping generation, which a generation-only
// filter drops.
func DeletionTimestampChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return true },
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
	}
}

// NodeLabelPrefixChangedPredicate returns a predicate that triggers on
// create/delete when the object carries labels with the given prefix, and on
// update only when labels with that prefix change. exceptKeys are excluded
// from both the prefix match and the diff, so a controller can watch a whole
// label namespace while ignoring the writes it makes into it.
func NodeLabelPrefixChangedPredicate(prefix string, exceptKeys ...string) predicate.Funcs {
	filtered := func(labels map[string]string) map[string]string {
		m := FilterMapWithPrefix(labels, prefix)
		for _, k := range exceptKeys {
			delete(m, k)
		}
		return m
	}
	hasPrefix := func(labels map[string]string) bool {
		return len(filtered(labels)) > 0
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return hasPrefix(e.Object.GetLabels()) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return hasPrefix(e.Object.GetLabels()) },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			old := filtered(e.ObjectOld.GetLabels())
			cur := filtered(e.ObjectNew.GetLabels())
			return !reflect.DeepEqual(old, cur)
		},
	}
}
