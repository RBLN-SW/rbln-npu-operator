package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/registry"
)

// DriverPatcher implementations that do not manage DaemonSets return
// (nil, nil) from PoolStatuses so the service can iterate uniformly.
type DriverPatcher interface {
	Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error
	PoolStatuses(ctx context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, error)
	ComponentName() string
	ComponentNamespace() string
	// Diagnostics reports pool failures from the last Patch call that
	// produced no DaemonSet.
	Diagnostics() PoolDiagnostics
}

// ImageChecker reports whether a composed driver image exists in its
// registry. Implemented by internal/registry.Checker; faked in tests.
// The error return is diagnostic detail for the verdict, not a failure.
type ImageChecker interface {
	Check(ctx context.Context, imageRef string, pullSecrets []corev1.Secret) (registry.Verdict, error)
}

// MissingImagePool identifies a pool whose composed driver image is absent
// from its registry (authoritative 404).
type MissingImagePool struct {
	Pool  string
	Image string
}

// PoolDiagnostics carries reconcile-time pool failures that produce no
// DaemonSet and would otherwise be invisible to PoolStatuses.
type PoolDiagnostics struct {
	NodesWithoutFamily []string
	MissingImagePools  []MissingImagePool
	// UnreadablePullSecrets lists configured spec.imagePullSecrets the operator
	// could not read, each with its API reason. The image check then ran
	// anonymously, which is the one way a NotFound verdict can be wrong, so a
	// MissingImagePools report alongside this list must not claim the image is
	// definitely absent.
	UnreadablePullSecrets []string
}

// basePatcher holds the dependencies and identity shared by every driver
// component patcher. The shared resources it reconciles (ServiceAccount,
// Role, RoleBinding) carry a non-controller ownerReference per RBLNDriver
// instance, so GC deletes them only once every owner is gone.
type basePatcher struct {
	client client.Client
	// apiReader bypasses the informer cache for status reads issued in the
	// same reconcile pass as a Delete; the cached client would otherwise
	// return the just-deleted object until the watch event propagates.
	apiReader        client.Reader
	log              logr.Logger
	scheme           *runtime.Scheme
	name             string // component name (e.g. "rbln-driver")
	instanceName     string // RBLNDriver instance name
	namespace        string
	openshiftVersion string
}

// ---------------------------------------------------------------------------
// DriverPatcher interface – generic implementations
// ---------------------------------------------------------------------------

func (b *basePatcher) ComponentName() string      { return b.instanceName }
func (b *basePatcher) ComponentNamespace() string { return b.namespace }

// ---------------------------------------------------------------------------
// Shared reconcile helpers
// ---------------------------------------------------------------------------

func (b *basePatcher) reconcileServiceAccount(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	}
	res, err := controllerutil.CreateOrPatch(ctx, b.client, sa, func() error {
		return controllerutil.SetOwnerReference(owner, sa, b.scheme)
	})
	if err != nil {
		b.log.Error(err, "Failed to reconcile ServiceAccount", "name", b.name)
		return err
	}
	b.log.Info("Reconciled ServiceAccount", "name", sa.Name, "namespace", sa.Namespace, "result", res)
	return nil
}

// reconcileOpenShiftRBAC creates the Role and RoleBinding required for
// OpenShift SCC access.  It is a no-op on non-OpenShift clusters.
func (b *basePatcher) reconcileOpenShiftRBAC(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	if b.openshiftVersion == "" {
		return nil
	}
	if err := b.reconcileRole(ctx, owner); err != nil {
		return err
	}
	return b.reconcileRoleBinding(ctx, owner)
}

func (b *basePatcher) reconcileRole(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	}
	res, err := controllerutil.CreateOrPatch(ctx, b.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			},
		}
		return controllerutil.SetOwnerReference(owner, role, b.scheme)
	})
	if err != nil {
		b.log.Error(err, "Failed to reconcile Role", "name", b.name)
		return err
	}
	b.log.Info("Reconciled Role", "name", role.Name, "namespace", role.Namespace, "result", res)
	return nil
}

func (b *basePatcher) reconcileRoleBinding(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	}
	res, err := controllerutil.CreateOrPatch(ctx, b.client, rb, func() error {
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     b.name,
		}
		rb.Subjects = []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: b.name, Namespace: b.namespace},
		}
		return controllerutil.SetOwnerReference(owner, rb, b.scheme)
	})
	if err != nil {
		b.log.Error(err, "Failed to reconcile RoleBinding", "name", b.name)
		return err
	}
	b.log.Info("Reconciled RoleBinding", "name", rb.Name, "namespace", rb.Namespace, "result", res)
	return nil
}

// ---------------------------------------------------------------------------
// Shared cleanup helpers
// ---------------------------------------------------------------------------

// deleteIfExists deletes the object, treating NotFound as success.
func (b *basePatcher) deleteIfExists(ctx context.Context, obj client.Object) error {
	if err := b.client.Delete(ctx, obj); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	return nil
}
