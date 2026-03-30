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
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
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

	cpNS := ""
	if clusterPolicy != nil {
		cpNS = clusterPolicy.Spec.Namespace
	}
	namespace, err := k8sutil.ResolveNamespace(cpNS, os.Getenv("OPERATOR_NAMESPACE"), driver.Namespace)
	if err != nil {
		s.log.Error(err, "namespace configuration error")
		return nil, err
	}
	s.namespace = namespace

	dmp, err := components.NewDriverManagerPatcher(client, log, s.namespace, driver, scheme, s.openshiftVersion)
	if err != nil {
		return nil, err
	}
	s.patcher = append(s.patcher, dmp)

	return s, nil
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
