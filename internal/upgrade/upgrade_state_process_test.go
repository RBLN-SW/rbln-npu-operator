package upgrade

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// ---------------------------------------------------------------------------
// ProcessDoneOrUnknownNodes
// ---------------------------------------------------------------------------

func TestProcessDoneOrUnknownNodes(t *testing.T) {
	tests := map[string]struct {
		stateName      string
		podRevHash     string
		dsRevHash      string
		annotations    map[string]string
		wantState      string
		safeDriverWait bool
	}{
		"unknown node with pod in sync transitions to Done": {
			stateName:  UpgradeStateUnknown,
			podRevHash: "rev1",
			dsRevHash:  "rev1",
			wantState:  UpgradeStateDone,
		},
		"unknown node with pod out of sync transitions to UpgradeRequired": {
			stateName:  UpgradeStateUnknown,
			podRevHash: "rev1",
			dsRevHash:  "rev2",
			wantState:  UpgradeStateUpgradeRequired,
		},
		"done node with pod in sync stays done": {
			stateName:  UpgradeStateDone,
			podRevHash: "rev1",
			dsRevHash:  "rev1",
			wantState:  UpgradeStateDone, // no transition expected
		},
		"done node with pod out of sync transitions to UpgradeRequired": {
			stateName:  UpgradeStateDone,
			podRevHash: "rev1",
			dsRevHash:  "rev2",
			wantState:  UpgradeStateUpgradeRequired,
		},
		"node waiting for safe driver load transitions to UpgradeRequired": {
			stateName:      UpgradeStateDone,
			podRevHash:     "rev1",
			dsRevHash:      "rev1",
			safeDriverWait: true,
			wantState:      UpgradeStateUpgradeRequired,
		},
		"node with upgrade-requested annotation transitions to UpgradeRequired": {
			stateName:  UpgradeStateDone,
			podRevHash: "rev1",
			dsRevHash:  "rev1",
			annotations: map[string]string{
				UpgradeRequestedAnnotationKey: trueString,
			},
			wantState: UpgradeStateUpgradeRequired,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pm := &mockPodManager{podRevisionHash: tc.podRevHash, dsRevisionHash: tc.dsRevHash}
			sdlm := &mockSafeDriverLoadManager{waiting: tc.safeDriverWait}
			mgr := newTestManager(t, withPodManager(pm), withSafeDriverLoadManager(sdlm))

			ns := newNodeUpgradeState("node-1", tc.stateName, tc.podRevHash)
			if tc.annotations != nil {
				ns.Node.Annotations = tc.annotations
			}
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				tc.stateName: {ns},
			})

			err := mgr.processDoneOrUnknownNodes(context.Background(), state, tc.stateName)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}

			gotState := updated.Labels[UpgradeStateLabelKey]
			if gotState != tc.wantState {
				t.Fatalf("node state = %q, want %q", gotState, tc.wantState)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProcessUpgradeRequiredNodes
// ---------------------------------------------------------------------------

func TestProcessUpgradeRequiredNodes(t *testing.T) {
	tests := map[string]struct {
		nodeCount           int
		maxParallelUpgrades int
		skipNodes           []string
		wantCordonCount     int
	}{
		"all nodes transition to CordonRequired with no limit": {
			nodeCount:           3,
			maxParallelUpgrades: 0,
			wantCordonCount:     3,
		},
		"respects max parallel upgrades limit": {
			nodeCount:           5,
			maxParallelUpgrades: 2,
			wantCordonCount:     2,
		},
		"skips nodes with skip label": {
			nodeCount:           3,
			maxParallelUpgrades: 0,
			skipNodes:           []string{"node-1"},
			wantCordonCount:     2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mgr := newTestManager(t)

			nodeStates := make([]*NodeUpgradeState, tc.nodeCount)
			for i := range tc.nodeCount {
				nodeName := fmt.Sprintf("node-%d", i)
				ns := newNodeUpgradeState(nodeName, UpgradeStateUpgradeRequired, "rev1")
				for _, skip := range tc.skipNodes {
					if nodeName == skip {
						ns.Node.Labels[UpgradeSkipNodeLabelKey] = trueString
					}
				}
				nodeStates[i] = ns
				registerNodes(t, mgr, ns.Node)
			}

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateUpgradeRequired: nodeStates,
			})

			policy := &v1beta1.DriverUpgradePolicySpec{
				MaxParallelUpgrades: tc.maxParallelUpgrades,
			}

			err := mgr.ProcessUpgradeRequiredNodes(context.Background(), state, policy)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			cordonCount := 0
			for i := range tc.nodeCount {
				var updated corev1.Node
				nodeName := fmt.Sprintf("node-%d", i)
				if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, &updated); err != nil {
					t.Fatalf("get node %s: %v", nodeName, err)
				}
				if updated.Labels[UpgradeStateLabelKey] == UpgradeStateCordonRequired {
					cordonCount++
				}
			}

			if cordonCount != tc.wantCordonCount {
				t.Fatalf("cordon count = %d, want %d", cordonCount, tc.wantCordonCount)
			}
		})
	}
}

func TestProcessUpgradeRequiredNodes_CountsInProgressFromNodeLabels(t *testing.T) {
	mgr := newTestManager(t)

	inProgressA := newNodeUpgradeState("node-reboot-a", UpgradeStateRebootValidationRequired, "rev1")
	inProgressB := newNodeUpgradeState("node-reboot-b", UpgradeStateRebootValidationRequired, "rev1")
	queued := newNodeUpgradeState("node-queued", UpgradeStateUpgradeRequired, "rev1")

	registerNodes(t, mgr, inProgressA.Node, inProgressB.Node, queued.Node)

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateUpgradeRequired: {queued},
	})

	policy := &v1beta1.DriverUpgradePolicySpec{MaxParallelUpgrades: 2}

	if err := mgr.ProcessUpgradeRequiredNodes(context.Background(), state, policy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated corev1.Node
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-queued"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateUpgradeRequired {
		t.Fatalf("queued node state = %q, want %q (cap of 2 already full, must not admit)",
			got, UpgradeStateUpgradeRequired)
	}
}

// ---------------------------------------------------------------------------
// ProcessCordonRequiredNodes
// ---------------------------------------------------------------------------

func TestProcessCordonRequiredNodes(t *testing.T) {
	tests := map[string]struct {
		cordonErr error
		wantErr   bool
		wantState string
	}{
		"cordon succeeds and transitions to WaitForJobsRequired": {
			wantState: UpgradeStateWaitForJobsRequired,
		},
		"cordon failure returns error": {
			cordonErr: fmt.Errorf("cordon failed"),
			wantErr:   true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cm := &mockCordonManager{cordonErr: tc.cordonErr}
			mgr := newTestManager(t, withCordonManager(cm))

			ns := newNodeUpgradeState("node-1", UpgradeStateCordonRequired, "rev1")
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateCordonRequired: {ns},
			})

			err := mgr.ProcessCordonRequiredNodes(context.Background(), state)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("node state = %q, want %q", got, tc.wantState)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProcessUncordonRequiredNodes
// ---------------------------------------------------------------------------

func TestProcessUncordonRequiredNodes(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		uncordonErr error
		wantErr     bool
		wantState   string
	}{
		"uncordon succeeds and transitions to Done": {
			wantState: UpgradeStateDone,
		},
		"uncordon failure returns error": {
			uncordonErr: fmt.Errorf("uncordon failed"),
			wantErr:     true,
		},
		"skips uncordon for nodes in requestor mode": {
			annotations: map[string]string{
				UpgradeRequestorModeAnnotationKey: trueString,
			},
			wantState: UpgradeStateUncordonRequired, // no transition
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cm := &mockCordonManager{uncordonErr: tc.uncordonErr}
			mgr := newTestManager(t, withCordonManager(cm))

			ns := newNodeUpgradeState("node-1", UpgradeStateUncordonRequired, "rev1")
			if tc.annotations != nil {
				ns.Node.Annotations = tc.annotations
			}
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateUncordonRequired: {ns},
			})

			err := mgr.ProcessUncordonRequiredNodes(context.Background(), state)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("node state = %q, want %q", got, tc.wantState)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProcessDrainNodes
// ---------------------------------------------------------------------------

func TestProcessDrainNodes(t *testing.T) {
	tests := map[string]struct {
		drainSpec *v1beta1.DrainSpec
		drainErr  error
		wantState string
		wantErr   bool
	}{
		"nil drain spec skips to PodRestartRequired": {
			drainSpec: nil,
			wantState: UpgradeStatePodRestartRequired,
		},
		"disabled drain spec skips to PodRestartRequired": {
			drainSpec: &v1beta1.DrainSpec{Enable: false},
			wantState: UpgradeStatePodRestartRequired,
		},
		"enabled drain spec calls drain manager": {
			drainSpec: &v1beta1.DrainSpec{Enable: true},
			wantState: UpgradeStateDrainRequired, // drain is async, state stays
		},
		"drain manager error propagates": {
			drainSpec: &v1beta1.DrainSpec{Enable: true},
			drainErr:  fmt.Errorf("drain failed"),
			wantErr:   true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dm := &mockDrainManager{err: tc.drainErr}
			mgr := newTestManager(t, withDrainManager(dm))

			ns := newNodeUpgradeState("node-1", UpgradeStateDrainRequired, "rev1")
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateDrainRequired: {ns},
			})

			err := mgr.ProcessDrainNodes(context.Background(), state, tc.drainSpec)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.drainSpec == nil || !tc.drainSpec.Enable {
				var updated corev1.Node
				if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
					t.Fatalf("get node: %v", err)
				}
				if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
					t.Fatalf("node state = %q, want %q", got, tc.wantState)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProcessValidationRequiredNodes
// ---------------------------------------------------------------------------

func TestProcessValidationRequiredNodes(t *testing.T) {
	tests := map[string]struct {
		validationDone bool
		validationErr  error
		wantState      string
		wantErr        bool
	}{
		"validation complete transitions to UncordonRequired": {
			validationDone: true,
			wantState:      UpgradeStateUncordonRequired,
		},
		"validation not complete stays in ValidationRequired": {
			validationDone: false,
			wantState:      UpgradeStateValidationRequired,
		},
		"validation error propagates": {
			validationErr: fmt.Errorf("validation failed"),
			wantErr:       true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			vm := &mockValidationManager{done: tc.validationDone, err: tc.validationErr}
			mgr := newTestManager(t, withValidationManager(vm), withValidationEnabled())

			ns := newNodeUpgradeState("node-1", UpgradeStateValidationRequired, "rev1")
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateValidationRequired: {ns},
			})

			err := mgr.ProcessValidationRequiredNodes(context.Background(), state)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("node state = %q, want %q", got, tc.wantState)
			}
		})
	}
}

func TestProcessValidationRequiredNodesContinuesPastNodeError(t *testing.T) {
	vm := &mockValidationManager{done: true, errOn: "node-bad"}
	mgr := newTestManager(t, withValidationManager(vm), withValidationEnabled())

	bad := newNodeUpgradeState("node-bad", UpgradeStateValidationRequired, "rev1")
	good := newNodeUpgradeState("node-good", UpgradeStateValidationRequired, "rev1")
	registerNodes(t, mgr, bad.Node, good.Node)

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateValidationRequired: {bad, good},
	})

	if err := mgr.ProcessValidationRequiredNodes(context.Background(), state); err == nil {
		t.Fatal("expected the bad node's validation error to be reported")
	}

	var updated corev1.Node
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-good"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateUncordonRequired {
		t.Fatalf("good node state = %q, want %q (one node's error must not block the rest)",
			got, UpgradeStateUncordonRequired)
	}
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-bad"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateValidationRequired {
		t.Fatalf("bad node state = %q, want unchanged %q", got, UpgradeStateValidationRequired)
	}
}

// ---------------------------------------------------------------------------
// Failure diagnosis recording
// ---------------------------------------------------------------------------

func TestProcessPodRestartNodesCrashLoopRecordsDiagnosis(t *testing.T) {
	ns := newNodeUpgradeState("node-1", UpgradeStatePodRestartRequired, "rev1")
	ns.DriverPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "driver",
		Ready:        false,
		RestartCount: MaxPodRestartCount + 1,
	}}
	mgr := newTestManager(t)
	registerNodes(t, mgr, ns.Node)

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStatePodRestartRequired: {ns},
	})
	if err := mgr.ProcessPodRestartNodes(context.Background(), state, false); err != nil {
		t.Fatalf("ProcessPodRestartNodes: %v", err)
	}

	var updated corev1.Node
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateFailed {
		t.Fatalf("state = %q, want %q", got, UpgradeStateFailed)
	}
	if reason := updated.Annotations[UpgradeFailureReasonAnnotationKey]; !strings.Contains(reason, "crash-looping") {
		t.Fatalf("failure reason = %q, want it to mention crash-looping", reason)
	}
	if got := updated.Annotations[UpgradeFailureStepAnnotationKey]; got != UpgradeStatePodRestartRequired {
		t.Fatalf("failure step = %q, want %q", got, UpgradeStatePodRestartRequired)
	}
}

func TestProcessRebootRequiredNodesTriggerFailureRecordsDiagnosis(t *testing.T) {
	mgr := newTestManager(t)
	mgr.rebootManager = &mockRebootManager{err: fmt.Errorf("reboot pod create rejected")}

	ns := newNodeUpgradeState("node-1", UpgradeStateRebootRequired, "rev1")
	ns.Node.Status.NodeInfo.BootID = "boot-1"
	registerNodes(t, mgr, ns.Node)

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateRebootRequired: {ns},
	})
	if err := mgr.ProcessRebootRequiredNodes(context.Background(), "test-ns", state,
		&v1beta1.RebootSpec{Enable: true}); err == nil {
		t.Fatal("expected the reboot trigger error to be reported")
	}

	var updated corev1.Node
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateFailed {
		t.Fatalf("state = %q, want %q", got, UpgradeStateFailed)
	}
	if reason := updated.Annotations[UpgradeFailureReasonAnnotationKey]; !strings.Contains(reason, "reboot pod create rejected") {
		t.Fatalf("failure reason = %q, want it to carry the trigger error", reason)
	}
	if got := updated.Annotations[UpgradeFailureStepAnnotationKey]; got != UpgradeStateRebootRequired {
		t.Fatalf("failure step = %q, want %q", got, UpgradeStateRebootRequired)
	}
}

// ---------------------------------------------------------------------------
// ProcessUpgradeFailedNodes
// ---------------------------------------------------------------------------

func TestProcessUpgradeFailedNodes(t *testing.T) {
	tests := map[string]struct {
		podRevHash  string
		dsRevHash   string
		podPhase    corev1.PodPhase
		podReady    bool
		annotations map[string]string
		wantState   string
	}{
		"pod in sync transitions to UncordonRequired": {
			podRevHash: "rev1",
			dsRevHash:  "rev1",
			podPhase:   corev1.PodRunning,
			podReady:   true,
			wantState:  UpgradeStateUncordonRequired,
		},
		"pod in sync with initial state annotation transitions to Done": {
			podRevHash: "rev1",
			dsRevHash:  "rev1",
			podPhase:   corev1.PodRunning,
			podReady:   true,
			annotations: map[string]string{
				UpgradeInitialStateAnnotationKey: trueString,
			},
			wantState: UpgradeStateDone,
		},
		"pod not in sync stays in Failed": {
			podRevHash: "rev1",
			dsRevHash:  "rev2",
			wantState:  UpgradeStateFailed,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pm := &mockPodManager{podRevisionHash: tc.podRevHash, dsRevisionHash: tc.dsRevHash}
			mgr := newTestManager(t, withPodManager(pm))

			ns := newNodeUpgradeState("node-1", UpgradeStateFailed, tc.podRevHash)
			if tc.annotations != nil {
				ns.Node.Annotations = tc.annotations
			}
			if tc.podPhase != "" {
				ns.DriverPod.Status.Phase = tc.podPhase
			}
			if tc.podReady {
				ns.DriverPod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Ready: true},
				}
			}
			registerNodes(t, mgr, ns.Node)

			state := newClusterState(map[string][]*NodeUpgradeState{
				UpgradeStateFailed: {ns},
			})

			err := mgr.ProcessUpgradeFailedNodes(context.Background(), state)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if got := updated.Labels[UpgradeStateLabelKey]; got != tc.wantState {
				t.Fatalf("node state = %q, want %q", got, tc.wantState)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isDriverPodFailing
// ---------------------------------------------------------------------------

func TestIsDriverPodFailing(t *testing.T) {
	mgr := newTestManager(t)

	tests := map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"pod with low restart count is not failing": {
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{Ready: false, RestartCount: 5},
					},
				},
			},
			want: false,
		},
		"pod with restart count above threshold is failing": {
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{Ready: false, RestartCount: MaxPodRestartCount + 1},
					},
				},
			},
			want: true,
		},
		"ready pod with high restart count is not failing": {
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{Ready: true, RestartCount: MaxPodRestartCount + 1},
					},
				},
			},
			want: false,
		},
		"init container above threshold is failing": {
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{Ready: false, RestartCount: MaxPodRestartCount + 1},
					},
				},
			},
			want: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := mgr.isDriverPodFailing(tc.pod); got != tc.want {
				t.Fatalf("isDriverPodFailing() = %t, want %t", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isRebootPostPodReady (standalone function)
// ---------------------------------------------------------------------------

func TestIsRebootPostPodReady(t *testing.T) {
	now := metav1.Now()
	tests := map[string]struct {
		pod  corev1.Pod
		want bool
	}{
		"running pod with all containers ready": {
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{Ready: true},
					},
				},
			},
			want: true,
		},
		"pod being deleted": {
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status: corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
				},
			},
			want: false,
		},
		"pod not running": {
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{{Ready: false}},
				},
			},
			want: false,
		},
		"pod with no container statuses": {
			pod: corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: false,
		},
		"pod with not-ready container": {
			pod: corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{Ready: true},
						{Ready: false},
					},
				},
			},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isRebootPostPodReady(tc.pod); got != tc.want {
				t.Fatalf("isRebootPostPodReady() = %t, want %t", got, tc.want)
			}
		})
	}
}
