package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

type ReadinessReport struct {
	State   rblnv1beta1.ComponentState
	Desired int32
	Ready   int32
	Message string
}

// Patcher manages the lifecycle of a single operator component.
type Patcher interface {
	IsEnabled() bool
	Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error
	CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error
	IsReady(ctx context.Context, nodeCountForType int32) ReadinessReport
	ComponentName() string
	ComponentNamespace() string
	WorkloadType() string
}

// basePatcher holds the dependencies and identity shared by every component patcher.
// Concrete patchers embed this struct and inherit its interface implementations.
type basePatcher struct {
	client           client.Client
	log              logr.Logger
	scheme           *runtime.Scheme
	name             string
	namespace        string
	openshiftVersion string
	enabled          bool
	workloadType     string
}

// ---------------------------------------------------------------------------
// Patcher interface – generic implementations
// ---------------------------------------------------------------------------

func (b *basePatcher) IsEnabled() bool            { return b.enabled }
func (b *basePatcher) ComponentName() string      { return b.name }
func (b *basePatcher) ComponentNamespace() string { return b.namespace }
func (b *basePatcher) WorkloadType() string       { return b.workloadType }

// IsReady is called by AssembleStatus only after PatchComponents has run for
// every enabled component, so the DaemonSet is expected to exist. A NotFound
// here therefore signals an unexpected deletion or stale cache.
func (b *basePatcher) IsReady(ctx context.Context, nodeCountForType int32) ReadinessReport {
	var ds appsv1.DaemonSet
	if err := b.client.Get(ctx, types.NamespacedName{Name: b.name, Namespace: b.namespace}, &ds); err != nil {
		return ReadinessReport{
			State:   rblnv1beta1.ComponentStateNotReady,
			Message: fmt.Sprintf("DaemonSet %s/%s not found: %v", b.namespace, b.name, err),
		}
	}

	desired := ds.Status.DesiredNumberScheduled
	ready := ds.Status.NumberReady

	if nodeCountForType == 0 {
		return ReadinessReport{State: rblnv1beta1.ComponentStateReady, Desired: desired, Ready: ready}
	}

	if desired == 0 {
		return ReadinessReport{
			State:   rblnv1beta1.ComponentStateNotReady,
			Desired: desired,
			Ready:   ready,
			Message: fmt.Sprintf(
				"DaemonSet %s/%s has 0 desired pods despite %d %s node(s) — label or selector mismatch",
				b.namespace, b.name, nodeCountForType, b.workloadType,
			),
		}
	}

	if ready != desired || ds.Status.NumberUnavailable > 0 {
		return ReadinessReport{
			State:   rblnv1beta1.ComponentStateNotReady,
			Desired: desired,
			Ready:   ready,
			Message: fmt.Sprintf(
				"DaemonSet %s/%s is progressing: %d of %d pods are Ready (%d unavailable)",
				b.namespace, b.name, ready, desired, ds.Status.NumberUnavailable,
			),
		}
	}

	return ReadinessReport{State: rblnv1beta1.ComponentStateReady, Desired: desired, Ready: ready}
}

// ---------------------------------------------------------------------------
// Shared init container builders
// ---------------------------------------------------------------------------

func buildToolkitValidationInitContainer(validatorSpec rblnv1beta1.ValidatorSpec) *corev1.Container {
	return k8sutil.NewContainerBuilder().
		WithName("toolkit-validation").
		WithImage(k8sutil.ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{"sh", "-c"}).
		WithArgs([]string{"until [ -f " + consts.ValidationsMountPath + "/toolkit-ready ]; do echo waiting for rbln container stack to be setup; sleep 5; done"}).
		WithSecurityContext(&corev1.SecurityContext{Privileged: ptr(true)}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             consts.ValidationsVolumeName,
				MountPath:        consts.ValidationsMountPath,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
		}).
		Build()
}

func buildVFIOPCIValidationInitContainer(validatorSpec rblnv1beta1.ValidatorSpec) *corev1.Container {
	return k8sutil.NewContainerBuilder().
		WithName("vfio-pci-validation").
		WithImage(k8sutil.ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{"rbln-validator"}).
		WithArgs([]string{"vfio-pci"}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
			{Name: "host-sys", MountPath: "/sys"},
		}).
		Build()
}

func buildRBLNBindingValidationInitContainer(validatorSpec rblnv1beta1.ValidatorSpec) *corev1.Container {
	return k8sutil.NewContainerBuilder().
		WithName("rbln-binding-validation").
		WithImage(k8sutil.ComposeImageReference(validatorSpec.Registry, validatorSpec.Image), validatorSpec.Version, validatorSpec.ImagePullPolicy).
		WithCommands([]string{"rbln-validator"}).
		WithArgs([]string{"vfio-pci", "assert-rbln"}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{Name: consts.ValidationsVolumeName, MountPath: consts.ValidationsMountPath},
			{Name: "host-sys", MountPath: "/sys"},
		}).
		Build()
}

// ---------------------------------------------------------------------------
// Shared reconcile helpers
// ---------------------------------------------------------------------------

func (b *basePatcher) reconcileServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	}

	res, err := controllerutil.CreateOrPatch(ctx, b.client, sa, func() error {
		return ctrl.SetControllerReference(owner, sa, b.scheme)
	})
	if err != nil {
		b.log.Error(err, "Failed to reconcile ServiceAccount", "name", b.name)
		return err
	}
	b.log.Info("Reconciled ServiceAccount", "name", sa.Name, "namespace", sa.Namespace, "result", res)
	return nil
}

// reconcileOpenShiftRBAC creates the Role and RoleBinding required for OpenShift SCC access.
// It is a no-op on non-OpenShift clusters.
func (b *basePatcher) reconcileOpenShiftRBAC(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if b.openshiftVersion == "" {
		return nil
	}
	if err := b.reconcileRole(ctx, owner); err != nil {
		return err
	}
	return b.reconcileRoleBinding(ctx, owner)
}

func (b *basePatcher) reconcileRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
		return ctrl.SetControllerReference(owner, role, b.scheme)
	})
	if err != nil {
		b.log.Error(err, "Failed to reconcile Role", "name", b.name)
		return err
	}
	b.log.Info("Reconciled Role", "name", role.Name, "namespace", role.Namespace, "result", res)
	return nil
}

func (b *basePatcher) reconcileRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
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
			{
				Kind:      "ServiceAccount",
				Name:      b.name,
				Namespace: b.namespace,
			},
		}
		return ctrl.SetControllerReference(owner, rb, b.scheme)
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

// deleteIfExists deletes the object, treating NotFound as a success.
func (b *basePatcher) deleteIfExists(ctx context.Context, obj client.Object) error {
	if err := b.client.Delete(ctx, obj); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteOpenShiftRBAC removes the namespaced Role and RoleBinding.
// It is a no-op on non-OpenShift clusters.
func (b *basePatcher) deleteOpenShiftRBAC(ctx context.Context) error {
	if b.openshiftVersion == "" {
		return nil
	}
	if err := b.deleteIfExists(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	}); err != nil {
		return err
	}
	return b.deleteIfExists(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	})
}

// deleteDaemonSet removes the DaemonSet owned by this component.
func (b *basePatcher) deleteDaemonSet(ctx context.Context) error {
	return b.deleteIfExists(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	})
}

// deleteServiceAccount removes the ServiceAccount owned by this component.
func (b *basePatcher) deleteServiceAccount(ctx context.Context) error {
	return b.deleteIfExists(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: b.name, Namespace: b.namespace},
	})
}
