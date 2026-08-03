package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// recordEvent is nil-safe so reconcilers built without a recorder never panic.
func recordEvent(recorder record.EventRecorder, object runtime.Object, eventType, reason, message string) {
	if recorder == nil || object == nil {
		return
	}
	recorder.Event(object, eventType, reason, message)
}
