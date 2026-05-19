package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
)

type DriverService struct {
	client client.Client

	ctx              context.Context
	log              logr.Logger
	scheme           *runtime.Scheme
	singleton        *rebellionsaiv1alpha1.RBLNDriver
	namespace        string
	openshiftVersion string

	patcher []components.DriverPatcher
}

func NewDriverService(
	ctx context.Context,
	client client.Client,
	apiReader client.Reader,
	log logr.Logger,
	scheme *runtime.Scheme,
	driver *rebellionsaiv1alpha1.RBLNDriver,
	clusterPolicy *rblnv1beta1.RBLNClusterPolicy,
	openshiftVersion string,
) (*DriverService, error) {
	s := &DriverService{
		client:           client,
		ctx:              ctx,
		log:              log,
		scheme:           scheme,
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

	dmp, err := components.NewDriverManagerPatcher(client, apiReader, log, s.namespace, driver, scheme, s.openshiftVersion)
	if err != nil {
		return nil, err
	}
	s.patcher = append(s.patcher, dmp)

	return s, nil
}

// Namespace returns the namespace the service deploys operands into.
func (s *DriverService) Namespace() string { return s.namespace }

// IsReady returns nil when every enabled component reports ready, or the
// first non-ready error it encounters.
func (s *DriverService) IsReady(ctx context.Context) error {
	for _, p := range s.patcher {
		if !p.IsEnabled() {
			continue
		}
		if err := p.IsReady(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AssembleStatus returns per-pool DaemonSet readiness and the aggregate
// desired/ready node counts across all enabled patchers. Status writing is
// the caller's responsibility.
func (s *DriverService) AssembleStatus(ctx context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, int32, int32, error) {
	// Start with an empty, non-nil slice so a "no pools" outcome propagates
	// as []{} rather than nil. applyDriverSummary uses nil as the sentinel
	// for "preserve previous values" on early-exit paths; a nil here would
	// silently keep stale node-pool data after cleanup.
	pools := make([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, 0)
	var totalDesired, totalReady int32
	for _, p := range s.patcher {
		if !p.IsEnabled() {
			continue
		}
		entries, err := p.PoolStatuses(ctx)
		if err != nil {
			return nil, 0, 0, err
		}
		for _, e := range entries {
			totalDesired += e.Desired
			totalReady += e.Ready
		}
		pools = append(pools, entries...)
	}
	return pools, totalDesired, totalReady, nil
}

// PatchComponents applies or removes each managed component according to
// whether it is enabled.
func (s *DriverService) PatchComponents(ctx context.Context) error {
	for _, p := range s.patcher {
		if p.IsEnabled() {
			if err := p.Patch(ctx, s.singleton); err != nil {
				return fmt.Errorf("patch %s: %w", p.ComponentName(), err)
			}
			continue
		}
		if err := p.CleanUp(ctx, s.singleton); err != nil {
			return fmt.Errorf("cleanup %s: %w", p.ComponentName(), err)
		}
	}
	return nil
}
