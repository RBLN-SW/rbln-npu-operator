package k8sutil

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestLogReconcileResult(t *testing.T) {
	tests := map[string]struct {
		result    controllerutil.OperationResult
		verbosity int
		wantLine  bool
		wantV     int
	}{
		"unchanged is hidden at default verbosity": {result: controllerutil.OperationResultNone, verbosity: 0, wantLine: false, wantV: 1},
		"unchanged shows at V(1)":                  {result: controllerutil.OperationResultNone, verbosity: 1, wantLine: true, wantV: 1},
		"created shows at default verbosity":       {result: controllerutil.OperationResultCreated, verbosity: 0, wantLine: true, wantV: 0},
		"updated shows at default verbosity":       {result: controllerutil.OperationResultUpdated, verbosity: 0, wantLine: true, wantV: 0},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var lines []string
			log := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{Verbosity: tc.verbosity})

			LogReconcileResult(log, "Reconciled ServiceAccount", tc.result, "name", "rbln-device-plugin")

			if got := len(lines) > 0; got != tc.wantLine {
				t.Fatalf("line emitted = %t, want %t (lines=%v)", got, tc.wantLine, lines)
			}
			if !tc.wantLine {
				return
			}
			if want := `"result"="` + string(tc.result) + `"`; !strings.Contains(lines[0], want) {
				t.Fatalf("line %q does not carry %s", lines[0], want)
			}
			if want := fmt.Sprintf(`"level"=%d`, tc.wantV); !strings.Contains(lines[0], want) {
				t.Fatalf("line %q not logged at %s", lines[0], want)
			}
		})
	}
}
