package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
)

type DriverService struct {
	singleton        *rebellionsaiv1alpha1.RBLNDriver
	namespace        string
	openshiftVersion string

	driverManager components.DriverPatcher
	smd           *components.SmdPatcher
}

// NewDriverService builds the per-reconcile component set for one RBLNDriver.
// ownedNodes is the owner resolver's node snapshot for this instance from the
// same reconcile pass; the driver-manager patcher partitions its pools from
// it instead of re-listing nodes, so the pool view cannot lag the resolver's
// owner-label writes.
func NewDriverService(
	ctx context.Context,
	client client.Client,
	apiReader client.Reader,
	log logr.Logger,
	scheme *runtime.Scheme,
	driver *rebellionsaiv1alpha1.RBLNDriver,
	clusterPolicy *rblnv1beta1.RBLNClusterPolicy,
	checker components.ImageChecker,
	openshiftVersion string,
	ownedNodes []corev1.Node,
) (*DriverService, error) {
	s := &DriverService{
		singleton:        driver,
		openshiftVersion: openshiftVersion,
	}

	namespace := os.Getenv("OPERATOR_NAMESPACE")
	if namespace == "" {
		namespace = driver.Namespace
	}
	if namespace == "" {
		return nil, fmt.Errorf("OPERATOR_NAMESPACE environment variable is not set")
	}
	s.namespace = namespace

	rdsEnabled := clusterPolicy != nil && clusterPolicy.Spec.RDS.Enabled

	dmp, err := components.NewDriverManagerPatcher(client, apiReader, log, s.namespace, driver, scheme, checker, s.openshiftVersion,
		ownedNodes, rdsEnabled)
	if err != nil {
		return nil, err
	}
	s.driverManager = dmp

	smd, err := components.NewSmdPatcher(client, apiReader, log, s.namespace, driver, scheme, s.openshiftVersion)
	if err != nil {
		return nil, err
	}
	s.smd = smd

	return s, nil
}

// Namespace returns the namespace the service deploys operands into.
func (s *DriverService) Namespace() string { return s.namespace }

// PoolDiagnostics returns the driver-manager patcher's pool failures from its
// last Patch call.
func (s *DriverService) PoolDiagnostics() components.PoolDiagnostics {
	return s.driverManager.Diagnostics()
}

// AssembleStatus returns per-pool DaemonSet readiness and the aggregate
// desired/ready node counts. Status writing is the caller's responsibility.
func (s *DriverService) AssembleStatus(ctx context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, int32, int32, error) {
	// Start with an empty, non-nil slice so a "no pools" outcome propagates
	// as []{} rather than nil. applyDriverSummary uses nil as the sentinel
	// for "preserve previous values" on early-exit paths; a nil here would
	// silently keep stale node-pool data after cleanup.
	pools := make([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, 0)
	var totalDesired, totalReady int32
	entries, err := s.driverManager.PoolStatuses(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	for _, e := range entries {
		totalDesired += e.Desired
		totalReady += e.Ready
	}
	pools = append(pools, entries...)
	return pools, totalDesired, totalReady, nil
}

// PatchComponents applies the managed components for this RBLNDriver. The smd
// patcher runs after the driver manager and is gated on it having rendered at
// least one driver DaemonSet: smd deploys only where the driver deploys.
// PoolStatuses reads through apiReader, so DaemonSets created or deleted by
// this same pass are observed.
func (s *DriverService) PatchComponents(ctx context.Context) error {
	if err := s.driverManager.Patch(ctx, s.singleton); err != nil {
		return fmt.Errorf("patch rbln-driver: %w", err)
	}
	pools, err := s.driverManager.PoolStatuses(ctx)
	if err != nil {
		return fmt.Errorf("gate rbln-smd on driver DaemonSets: %w", err)
	}
	if err := s.smd.Patch(ctx, s.singleton, len(pools) > 0); err != nil {
		return fmt.Errorf("patch rbln-smd: %w", err)
	}
	return nil
}

// SmdStatus reports the rbln-smd DaemonSet readiness; nil when it does not
// exist. Status writing is the caller's responsibility.
func (s *DriverService) SmdStatus(ctx context.Context) (*rebellionsaiv1alpha1.RBLNDriverSmdStatus, error) {
	return s.smd.Status(ctx)
}
