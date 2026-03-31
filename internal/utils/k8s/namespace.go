package k8sutil

import "fmt"

// ResolveNamespace returns the first non-empty string from candidates.
// Returns an error if all candidates are empty.
func ResolveNamespace(candidates ...string) (string, error) {
	for _, ns := range candidates {
		if ns != "" {
			return ns, nil
		}
	}
	return "", fmt.Errorf("namespace is not configured; set OPERATOR_NAMESPACE env variable or spec.namespace")
}
