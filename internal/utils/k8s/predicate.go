package k8sutil

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// NodeLabelChangedPredicate returns a predicate that triggers on create/delete
// and on update only when one of the specified label keys changes value.
func NodeLabelChangedPredicate(keys ...string) predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return true },
		DeleteFunc:  func(e event.DeleteEvent) bool { return true },
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

// NodeLabelPrefixChangedPredicate returns a predicate that triggers on
// create/delete when the object carries labels with the given prefix, and on
// update only when labels with that prefix change.
func NodeLabelPrefixChangedPredicate(prefix string) predicate.Funcs {
	hasPrefix := func(labels map[string]string) bool {
		return len(FilterMapWithPrefix(labels, prefix)) > 0
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return hasPrefix(e.Object.GetLabels()) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return hasPrefix(e.Object.GetLabels()) },
		GenericFunc: func(e event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			old := FilterMapWithPrefix(e.ObjectOld.GetLabels(), prefix)
			cur := FilterMapWithPrefix(e.ObjectNew.GetLabels(), prefix)
			return !reflect.DeepEqual(old, cur)
		},
	}
}
