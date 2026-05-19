package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterinfo"
	"github.com/rebellions-sw/rbln-npu-operator/internal/conditions"
	"github.com/rebellions-sw/rbln-npu-operator/internal/controller"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

func registerControllers(ctx context.Context, mgr ctrl.Manager) error {
	clusterInfo, err := clusterinfo.New(ctx, mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("get cluster info: %w", err)
	}
	setupLog.Info("openshift version", "version", clusterInfo.OpenShiftVersion)

	if err := registerClusterPolicyController(mgr, clusterInfo); err != nil {
		return err
	}
	if err := registerUpgradeController(ctx, mgr); err != nil {
		return err
	}
	if err := registerDriverController(mgr, clusterInfo); err != nil {
		return err
	}
	return nil
}

func registerClusterPolicyController(mgr ctrl.Manager, clusterInfo *clusterinfo.Info) error {
	if err := (&controller.RBLNClusterPolicyReconciler{
		Client:      mgr.GetClient(),
		Log:         ctrl.Log.WithName("controllers").WithName("RBLNClusterPolicy"),
		Scheme:      mgr.GetScheme(),
		ClusterInfo: clusterInfo,
		Conditions:  conditions.NewUpdater(mgr.GetClient()),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RBLNClusterPolicy: %w", err)
	}
	return nil
}

func registerUpgradeController(ctx context.Context, mgr ctrl.Manager) error {
	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		return fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}

	upgradeLogger := ctrl.Log.WithName("controllers").WithName("RBLNDriverUpgrade")

	stateManager, err := upgrade.NewClusterUpgradeStateManager(
		upgradeLogger,
		mgr.GetConfig(),
		mgr.GetEventRecorderFor(operatorName),
		mgr.GetScheme(),
	)
	if err != nil {
		return fmt.Errorf("create upgrade state manager: %w", err)
	}

	stateManager = stateManager.
		WithPodDeletionEnabled(npuPodSpecFilter).
		WithValidationEnabled(validatorPodSelector)

	if err := (&controller.UpgradeReconciler{
		Client:       mgr.GetClient(),
		Log:          upgradeLogger,
		Scheme:       mgr.GetScheme(),
		Namespace:    namespace,
		StateManager: stateManager,
	}).SetupWithManager(ctx, mgr); err != nil {
		return fmt.Errorf("setup Upgrade: %w", err)
	}
	return nil
}

func registerDriverController(mgr ctrl.Manager, clusterInfo *clusterinfo.Info) error {
	if err := (&controller.RBLNDriverReconciler{
		Client:      mgr.GetClient(),
		APIReader:   mgr.GetAPIReader(),
		Log:         ctrl.Log.WithName("controllers").WithName("RBLNDriver"),
		Scheme:      mgr.GetScheme(),
		ClusterInfo: clusterInfo,
		Conditions:  conditions.NewUpdater(mgr.GetClient()),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RBLNDriver: %w", err)
	}
	return nil
}

func npuPodSpecFilter(pod corev1.Pod) bool {
	npuInResourceList := func(rl corev1.ResourceList) bool {
		for resourceName := range rl {
			str := string(resourceName)
			if strings.HasPrefix(str, "rebellions.ai/") {
				return true
			}
		}
		return false
	}

	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
		return false
	}

	for _, c := range pod.Spec.Containers {
		if npuInResourceList(c.Resources.Limits) || npuInResourceList(c.Resources.Requests) {
			return true
		}
	}
	return false
}
