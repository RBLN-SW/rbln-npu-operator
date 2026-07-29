package upgrade

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// recordedEvent collects everything a FakeRecorder line encodes.
type recordedEvent struct {
	Type    string
	Reason  string
	Message string
}

// collectEvents drains the FakeRecorder channel without blocking forever.
func collectEvents(t *testing.T, rec *record.FakeRecorder, want int) []recordedEvent {
	t.Helper()
	events := make([]recordedEvent, 0, want)
	timeout := time.After(2 * time.Second)
	for len(events) < want {
		select {
		case raw := <-rec.Events:
			parts := strings.SplitN(raw, " ", 3)
			if len(parts) != 3 {
				t.Fatalf("unexpected event format: %q", raw)
			}
			events = append(events, recordedEvent{Type: parts[0], Reason: parts[1], Message: parts[2]})
		case <-timeout:
			t.Fatalf("timed out waiting for events: got %d, want %d", len(events), want)
		}
	}
	return events
}

// expectNoEvent asserts the recorder stays silent for a short window.
func expectNoEvent(t *testing.T, rec *record.FakeRecorder) {
	t.Helper()
	select {
	case raw := <-rec.Events:
		t.Fatalf("unexpected event: %q", raw)
	case <-time.After(100 * time.Millisecond):
	}
}

func expectEvent(t *testing.T, rec *record.FakeRecorder, eventType, reason, msgSubstring string) {
	t.Helper()
	got := collectEvents(t, rec, 1)[0]
	if got.Type != eventType || got.Reason != reason {
		t.Fatalf("event = %s/%s, want %s/%s", got.Type, got.Reason, eventType, reason)
	}
	if msgSubstring != "" && !strings.Contains(got.Message, msgSubstring) {
		t.Fatalf("event message %q does not contain %q", got.Message, msgSubstring)
	}
}

func TestRecordEventNilSafe(t *testing.T) {
	recordEvent(nil, nil, "Normal", "AnyReason", "no panic expected")
}

// spyRecorder captures involvedObject, which FakeRecorder discards.
type spyRecorder struct {
	mu      sync.Mutex
	objects []runtime.Object
	reasons []string
}

func (s *spyRecorder) Event(object runtime.Object, _, reason, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = append(s.objects, object)
	s.reasons = append(s.reasons, reason)
}

func (s *spyRecorder) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...interface{}) {
	s.Event(object, eventType, reason, fmt.Sprintf(messageFmt, args...))
}

func (s *spyRecorder) AnnotatedEventf(object runtime.Object, _ map[string]string, eventType, reason, messageFmt string, args ...interface{}) {
	s.Eventf(object, eventType, reason, messageFmt, args...)
}
