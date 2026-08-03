package upgrade

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// recordNodeEvent is nil-safe so test managers without a recorder never panic.
func recordNodeEvent(recorder record.EventRecorder, node *corev1.Node, eventType, reason, message string) {
	if recorder == nil {
		return
	}
	recorder.Event(node, eventType, reason, message)
}
