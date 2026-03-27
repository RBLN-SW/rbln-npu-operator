package k8sutil

import (
	"strings"
)

// Builder is a generic builder for any type T
type Builder[T any] struct {
	obj *T
}

func NewBuilder[T any](obj *T) *Builder[T] {
	return &Builder[T]{obj: obj}
}

func (b *Builder[T]) Build() *T {
	return b.obj
}

// MergeMaps merges two maps, with the second map taking precedence.
func MergeMaps(base, override map[string]string) map[string]string {
	if base == nil && override == nil {
		return nil
	}
	result := make(map[string]string)
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		result[k] = v
	}
	return result
}

// FilterMapWithPrefix returns a new map with certain prefix from old map
func FilterMapWithPrefix(k8sMap map[string]string, prefix string) map[string]string {
	r := make(map[string]string)
	for k, v := range k8sMap {
		if strings.HasPrefix(k, prefix) {
			r[k] = v
		}
	}
	return r
}
