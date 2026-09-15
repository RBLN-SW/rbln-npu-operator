package k8sutil

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// LogReconcileResult records a CreateOrPatch/CreateOrUpdate outcome. A no-op
// result is per-reconcile detail and goes to V(consts.VDebug); only an actual
// create or update is a state change worth an info line. keysAndValues follows
// logr's alternating key/value convention. The pairs are copied rather than
// appended in place so a caller forwarding its own slice never has its
// backing array overwritten.
func LogReconcileResult(log logr.Logger, msg string, res controllerutil.OperationResult, keysAndValues ...any) {
	kv := make([]any, 0, len(keysAndValues)+2)
	kv = append(kv, keysAndValues...)
	kv = append(kv, "result", res)
	if res == controllerutil.OperationResultNone {
		log.V(consts.VDebug).Info(msg, kv...)
		return
	}
	log.Info(msg, kv...)
}
