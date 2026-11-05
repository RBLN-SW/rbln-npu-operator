package main

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/RBLN-SW/rbln-npu-operator/internal/clusterinfo"
	"github.com/RBLN-SW/rbln-npu-operator/internal/conditions"
	"github.com/RBLN-SW/rbln-npu-operator/internal/controller"
	"github.com/RBLN-SW/rbln-npu-operator/internal/npuusage"
	"github.com/RBLN-SW/rbln-npu-operator/internal/upgrade"
)

func registerControllers(ctx context.Context, mgr ctrl.Manager) error {
	clusterInfo, err := clusterinfo.New(ctx, mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("get cluster info: %w", err)
	}
	setupLog.Info("OpenShift version", "version", clusterInfo.OpenShiftVersion)

	// One recorder shared by every controller, so all events report the same source.
	recorder := mgr.GetEventRecorderFor(operatorName)

	if err := registerClusterPolicyController(mgr, clusterInfo, recorder); err != nil {
		return err
	}
	if err := registerUpgradeController(ctx, mgr, recorder); err != nil {
		return err
	}
	if err := registerDriverController(mgr, clusterInfo, recorder); err != nil {
		return err
	}
	return nil
}

func registerClusterPolicyController(
	mgr ctrl.Manager, clusterInfo *clusterinfo.Info, recorder record.EventRecorder,
) error {
	if err := (&controller.RBLNClusterPolicyReconciler{
		Client:      mgr.GetClient(),
		APIReader:   mgr.GetAPIReader(),
		Log:         ctrl.Log.WithName("controllers").WithName("RBLNClusterPolicy"),
		Scheme:      mgr.GetScheme(),
		ClusterInfo: clusterInfo,
		Conditions:  conditions.NewUpdater(mgr.GetClient()),
		Recorder:    recorder,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RBLNClusterPolicy: %w", err)
	}
	return nil
}

func registerUpgradeController(ctx context.Context, mgr ctrl.Manager, recorder record.EventRecorder) error {
	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		return fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}

	upgradeLogger := ctrl.Log.WithName("controllers").WithName("RBLNDriverUpgrade")

	stateManager, err := upgrade.NewClusterUpgradeStateManager(
		upgradeLogger,
		mgr.GetConfig(),
		recorder,
		mgr.GetScheme(),
		upgrade.ManagerOptions{
			PodDeletionFilter:     npuPodSpecFilter,
			ValidationPodSelector: validatorPodSelector,
		},
	)
	if err != nil {
		return fmt.Errorf("create upgrade state manager: %w", err)
	}

	if err := (&controller.UpgradeReconciler{
		Client:       mgr.GetClient(),
		APIReader:    mgr.GetAPIReader(),
		Scheme:       mgr.GetScheme(),
		Namespace:    namespace,
		StateManager: stateManager,
	}).SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("setup Upgrade: %w", err)
	}
	return nil
}

func registerDriverController(mgr ctrl.Manager, clusterInfo *clusterinfo.Info, recorder record.EventRecorder) error {
	if err := (&controller.RBLNDriverReconciler{
		Client:      mgr.GetClient(),
		APIReader:   mgr.GetAPIReader(),
		Log:         ctrl.Log.WithName("controllers").WithName("RBLNDriver"),
		Scheme:      mgr.GetScheme(),
		ClusterInfo: clusterInfo,
		Conditions:  conditions.NewUpdater(mgr.GetClient()),
		Recorder:    recorder,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RBLNDriver: %w", err)
	}
	return nil
}

// npuPodSpecFilter applies the upgrade's phase policy to the shared resource
// detector. DRA consumers are inspected separately by the upgrade package.
func npuPodSpecFilter(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
		return false
	}
	return len(npuusage.ResourceNames(&pod)) > 0
}
