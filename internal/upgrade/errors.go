package upgrade

import "errors"

// ErrDriverDaemonSetHasUnscheduledPods is returned when the observed driver pod
// count does not match DaemonSet desired scheduling during state build.
var ErrDriverDaemonSetHasUnscheduledPods = errors.New("driver DaemonSet should not have unscheduled pods")
