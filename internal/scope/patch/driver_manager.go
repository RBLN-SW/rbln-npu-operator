package patch

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	k8sutils "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const (
	driverManagerName                         = "rbln-driver"
	driverManagerAppLabelKey                  = "app.kubernetes.io/component"
	driverManagerNodePoolLabelKey             = "rebellions.ai/driver-node-pool"
	driverManagerInstanceLabelKey             = "rebellions.ai/driver-instance"
	driverManagerDeployLabelKey               = "rebellions.ai/npu.deploy.driver"
	driverManagerInitContainer                = "k8s-driver-manager"
	driverManagerContainer                    = "rbln-driver-container"
	driverManagerCommand                      = "driver-manager"
	driverManagerSyncDriverLabel              = "reconcile-driver-state"
	driverConfigDigestEnv                     = "DRIVER_CONFIG_DIGEST"
	driverLastAppliedHashAnnotation           = "rebellions.ai/last-applied-hash"
	driverInstallerCommand                    = "/opt/rebellions/bin/rbln-driver"
	driverInstallerInitArg                    = "init"
	startupProbeConfigMapSuffix               = "startup-probe"
	startupProbeScriptName                    = "startup-probe.sh"
	startupProbeScriptPath                    = "/usr/local/bin/rbln-startup-probe.sh"
	driverManagerStartupProbePeriodSeconds    = 10
	driverManagerStartupProbeTimeoutSeconds   = 120
	driverManagerStartupProbeFailureThreshold = 60
	hostDriverVolumeName                      = "host-driver"
	hostDriverPath                            = "/run/rbln/driver"
	hostRootVolumeName                        = "host-root"
	hostRootPath                              = "/"
	hostDevVolumeName                         = "host-dev"
	hostDevPath                               = "/dev"
)

type mountPathToVolumeSource map[string]corev1.VolumeSource

var subscriptionPathMap = map[string]mountPathToVolumeSource{
	"rhel": {
		"/etc/pki/entitlement": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/pki/entitlement",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
		"/etc/rhsm": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/rhsm",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
		"/etc/yum.repos.d": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/yum.repos.d",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
	},
	"rhcos": {
		"/var/run/secrets/etc-pki-entitlement": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/pki/entitlement",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
		"/var/run/secrets/rhsm": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/rhsm",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
		"/etc/yum.repos.d": corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/yum.repos.d",
				Type: ptr(corev1.HostPathDirectory),
			},
		},
	},
}

func getSubscriptionPathsToVolumeSources(os string) (mountPathToVolumeSource, error) {
	if pathToVolumeSource, ok := subscriptionPathMap[os]; ok {
		return pathToVolumeSource, nil
	}
	return nil, fmt.Errorf("distribution %s not supported", os)
}

type driverManagerPatcher struct {
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rebellionsaiv1alpha1.RBLNDriverSpec
	name             string
	instanceName     string
	namespace        string
	openshiftVersion string
}

type DriverPatcher interface {
	IsEnabled() bool
	Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error
	CleanUp(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error
	ConditionReport(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) ([]metav1.Condition, error)
	ComponentName() string
	ComponentNamespace() string
}

func NewDriverManagerPatcher(client client.Client, log logr.Logger, namespace string, driver *rebellionsaiv1alpha1.RBLNDriver, scheme *runtime.Scheme, openshiftVersion string) (DriverPatcher, error) {
	if driver == nil {
		return nil, fmt.Errorf("driver is nil")
	}
	return &driverManagerPatcher{
		client:           client,
		log:              log,
		scheme:           scheme,
		desiredSpec:      &driver.Spec,
		name:             driverManagerName,
		instanceName:     driver.Name,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
	}, nil
}

func (h *driverManagerPatcher) IsEnabled() bool {
	return h.desiredSpec != nil
}

func (h *driverManagerPatcher) Patch(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	if !h.IsEnabled() {
		return nil
	}

	if err := h.handleServiceAccount(ctx); err != nil {
		return err
	}
	if h.openshiftVersion != "" {
		if err := h.handleRole(ctx); err != nil {
			return err
		}
		if err := h.handleRoleBinding(ctx); err != nil {
			return err
		}
	}
	if err := h.handleClusterRole(ctx); err != nil {
		return err
	}
	if err := h.handleClusterRoleBinding(ctx); err != nil {
		return err
	}
	if err := h.handleConfigMap(ctx); err != nil {
		return err
	}

	nodePools, err := getNodePools(ctx, h.client, h.desiredSpec.NodeSelector)
	if err != nil {
		return err
	}
	if len(nodePools) == 0 {
		h.log.Info("WARNING: no nodes matching the given selector for driver manager; skipping daemonset reconcile", "instance", h.instanceName)
		return nil
	}
	for _, nodePool := range nodePools {
		if err := h.handleDaemonSet(ctx, owner, nodePool); err != nil {
			return err
		}
	}

	return nil
}

func (h *driverManagerPatcher) CleanUp(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) error {
	h.log.Info("WARNING: Driver Manager is disabled. Remove all Driver Manager resources")
	dsList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, dsList, client.InNamespace(h.namespace), client.MatchingLabels(map[string]string{
		driverManagerAppLabelKey:      h.name,
		driverManagerInstanceLabelKey: h.instanceName,
	})); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	for _, ds := range dsList.Items {
		if err := h.client.Delete(ctx, &ds); err != nil && !kapierrors.IsNotFound(err) {
			return err
		}
	}
	otherInstancesExist, err := h.hasOtherDriverInstances(ctx, owner)
	if err != nil {
		return err
	}
	if otherInstancesExist {
		h.log.Info("Skip deleting shared driver manager resources because other RBLNDriver instances exist", "instance", h.instanceName)
		return nil
	}
	if err := h.client.Delete(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.startupProbeConfigMapName(),
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if h.openshiftVersion != "" {
		if err := h.client.Delete(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      h.name,
				Namespace: h.namespace,
			},
		}); err != nil && !kapierrors.IsNotFound(err) {
			return err
		}
		if err := h.client.Delete(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      h.name,
				Namespace: h.namespace,
			},
		}); err != nil && !kapierrors.IsNotFound(err) {
			return err
		}
	}
	if err := h.client.Delete(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func (h *driverManagerPatcher) ConditionReport(ctx context.Context, _ *rebellionsaiv1alpha1.RBLNDriver) ([]metav1.Condition, error) {
	dsList := &appsv1.DaemonSetList{}
	if err := h.client.List(ctx, dsList, client.InNamespace(h.namespace), client.MatchingLabels(map[string]string{
		driverManagerAppLabelKey:      h.name,
		driverManagerInstanceLabelKey: h.instanceName,
	})); err != nil {
		return []metav1.Condition{{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionFalse,
			Reason:             DaemonSetNotFound,
			Message:            fmt.Sprintf("DaemonSet list could not be retrieved: %v", err),
			LastTransitionTime: metav1.Now(),
		}}, nil
	}
	if len(dsList.Items) == 0 {
		return []metav1.Condition{{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionFalse,
			Reason:             DaemonSetNotFound,
			Message:            fmt.Sprintf("DaemonSet for %s/%s could not be found", h.namespace, h.instanceName),
			LastTransitionTime: metav1.Now(),
		}}, nil
	}

	notReady := make([]string, 0)
	for _, ds := range dsList.Items {
		ready := ds.Status.DesiredNumberScheduled > 0 &&
			ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
			ds.Status.NumberUnavailable == 0
		if ready {
			continue
		}
		notReady = append(notReady, fmt.Sprintf("%s/%s", ds.Namespace, ds.Name))
	}
	if len(notReady) > 0 {
		return []metav1.Condition{
			{
				Type:               DaemonSetReady,
				Status:             metav1.ConditionFalse,
				Reason:             DaemonSetPodsNotReady,
				Message:            fmt.Sprintf("DaemonSets not ready: %s", strings.Join(notReady, ", ")),
				LastTransitionTime: metav1.Now(),
			},
		}, nil
	}

	return []metav1.Condition{
		{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionTrue,
			Reason:             DaemonSetAllPodsReady,
			Message:            fmt.Sprintf("All pods in DaemonSets for %s are running", h.instanceName),
			LastTransitionTime: metav1.Now(),
		},
	}, nil
}

func (h *driverManagerPatcher) ComponentName() string {
	return h.instanceName
}

func (h *driverManagerPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *driverManagerPatcher) startupProbeConfigMapName() string {
	return fmt.Sprintf("%s-%s", h.name, startupProbeConfigMapSuffix)
}

func (h *driverManagerPatcher) handleConfigMap(ctx context.Context) error {
	builder := k8sutils.NewConfigMapBuilder(h.startupProbeConfigMapName(), h.namespace)
	cm := builder.Build()

	script := `#!/bin/sh
set -eu

VALIDATIONS_DIR="` + validationsMountPath + `"
READY_FILE="${VALIDATIONS_DIR}/.driver-ctr-ready"

mkdir -p "${VALIDATIONS_DIR}"

if [ ! -f /sys/module/rebellions/refcnt ]; then
  echo "Rebellions kernel module not loaded"
  exit 1
fi

if ! command -v rbln-smi >/dev/null 2>&1; then
  echo "rbln-smi not found"
  exit 1
fi

if ! rbln-smi; then
  echo "rbln-smi failed"
  exit 1
fi

TMP_FILE="${READY_FILE}.tmp"
: > "$TMP_FILE"
mv "$TMP_FILE" "$READY_FILE"
`

	cmRes, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm = builder.
			WithData(map[string]string{
				startupProbeScriptName: script,
			}).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager startup probe ConfigMap")
		return err
	}
	h.log.Info("Reconciled Driver Manager startup probe ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", cmRes)
	return nil
}

func (h *driverManagerPatcher) handleServiceAccount(ctx context.Context) error {
	builder := k8sutils.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager ServiceAccount")
		return err
	}
	h.log.Info("Reconciled Driver Manager ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *driverManagerPatcher) handleRole(ctx context.Context) error {
	builder := k8sutils.NewRoleBuilder(h.name, h.namespace)
	role := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role = builder.
			WithRules(rbacv1.PolicyRule{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			}).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager Role")
		return err
	}
	h.log.Info("Reconciled Driver Manager Role", "namespace", role.Namespace, "name", role.Name, "result", roleRes)
	return nil
}

func (h *driverManagerPatcher) handleRoleBinding(ctx context.Context) error {
	builder := k8sutils.NewRoleBindingBuilder(h.name, h.namespace)
	binding := builder.Build()

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding = builder.
			WithRoleRef(rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     h.name,
			}).
			WithSubjects(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.name,
				Namespace: h.namespace,
			}).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager RoleBinding")
		return err
	}
	h.log.Info("Reconciled Driver Manager RoleBinding", "namespace", binding.Namespace, "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *driverManagerPatcher) handleClusterRole(ctx context.Context) error {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/eviction"},
				Verbs:     []string{"create"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets"},
				Verbs:     []string{"get"},
			},
		}
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager ClusterRole")
		return err
	}
	h.log.Info("Reconciled Driver Manager ClusterRole", "name", role.Name, "result", roleRes)
	return nil
}

func (h *driverManagerPatcher) handleClusterRoleBinding(ctx context.Context) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: h.name,
		},
	}

	bindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, binding, func() error {
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     h.name,
		}
		binding.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.name,
				Namespace: h.namespace,
			},
		}
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile Driver Manager ClusterRoleBinding")
		return err
	}
	h.log.Info("Reconciled Driver Manager ClusterRoleBinding", "name", binding.Name, "result", bindingRes)
	return nil
}

func (h *driverManagerPatcher) handleDaemonSet(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver, pool nodePool) error {
	podSpec, err := h.buildDriverPodSpec(pool)
	if err != nil {
		return err
	}

	ds := k8sutils.NewDaemonSetBuilder(h.instanceName+"-"+pool.name, h.namespace).
		WithLabels(h.driverManagerLabels(pool)).
		WithLabelSelectors(h.driverManagerLabels(pool)).
		WithAnnotations(h.desiredSpec.Annotations).
		WithPodSpec(podSpec).
		WithUpdateStrategy(appsv1.DaemonSetUpdateStrategy{
			Type: appsv1.OnDeleteDaemonSetStrategyType,
		}).
		WithOwner(owner, h.scheme).
		Build()

	driverConfigDigest := GetObjectHash(ds.Spec.Template.Spec.Containers)
	ds.Spec.Template.Spec.InitContainers[0].Env = upsertEnvVar(
		ds.Spec.Template.Spec.InitContainers[0].Env,
		corev1.EnvVar{Name: driverConfigDigestEnv, Value: driverConfigDigest},
	)

	ds.Annotations = k8sutils.MergeMaps(ds.Annotations, map[string]string{
		driverLastAppliedHashAnnotation: driverConfigDigest,
	})

	current, err := h.getDaemonSet(ctx, ds.Name)
	if err != nil {
		if !kapierrors.IsNotFound(err) {
			return err
		}
		if err := h.client.Create(ctx, ds); err != nil {
			h.log.Error(err, "Failed to create Driver Manager DaemonSet")
			return err
		}
		h.log.Info("Reconciled Driver Manager DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", "created")
		return nil
	}
	skipUpdate := h.shouldSkipDaemonSetUpdateByDriverHash(current, driverConfigDigest)
	if skipUpdate {
		return nil
	}

	ds.SetResourceVersion(current.GetResourceVersion())
	ds.SetFinalizers(current.GetFinalizers())
	if err := h.client.Update(ctx, ds); err != nil {
		h.log.Error(err, "Failed to update Driver Manager DaemonSet")
		return err
	}
	h.log.Info("Reconciled Driver Manager DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", "updated")
	return nil
}

func (h *driverManagerPatcher) hasOtherDriverInstances(ctx context.Context, owner *rebellionsaiv1alpha1.RBLNDriver) (bool, error) {
	driverList := &rebellionsaiv1alpha1.RBLNDriverList{}
	if err := h.client.List(ctx, driverList); err != nil {
		return false, err
	}
	for _, driver := range driverList.Items {
		if owner != nil && driver.Name == owner.Name {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (h *driverManagerPatcher) buildDriverManagerInitContainer() *corev1.Container {
	return k8sutils.NewContainerBuilder().
		WithName(driverManagerInitContainer).
		WithImage(ComposeImageReference(
			h.desiredSpec.Manager.Registry, h.desiredSpec.Manager.Image),
			h.desiredSpec.Manager.Version,
			h.desiredSpec.Manager.ImagePullPolicy,
		).
		WithCommands([]string{driverManagerCommand}).
		WithArgs([]string{driverManagerSyncDriverLabel}).
		WithEnvs([]corev1.EnvVar{
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name:  "ENABLE_NPU_POD_EVICTION",
				Value: "true",
			},
			{
				Name:  "ENABLE_AUTO_DRAIN",
				Value: "false",
			},
			{
				Name:  "DRAIN_USE_FORCE",
				Value: "false",
			},
			{
				Name:  "DRAIN_POD_SELECTOR_LABEL",
				Value: "",
			},
			{
				Name:  "DRAIN_TIMEOUT_SECONDS",
				Value: "0s",
			},
			{
				Name:  "DRAIN_DELETE_EMPTYDIR_DATA",
				Value: "false",
			},
			{
				Name: "OPERATOR_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			SELinuxOptions: &corev1.SELinuxOptions{
				Level: "s0",
			},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             hostRootVolumeName,
				MountPath:        "/host",
				ReadOnly:         true,
				MountPropagation: ptr(corev1.MountPropagationHostToContainer),
			},
			{
				Name:      hostDevVolumeName,
				MountPath: "/dev",
				ReadOnly:  true,
			},
		}).
		Build()
}

func (h *driverManagerPatcher) buildDriverContainer(
	pool nodePool,
	additionalVolumeMounts []corev1.VolumeMount,
) (*corev1.Container, error) {
	driverContainer := k8sutils.NewContainerBuilder().
		WithName(driverManagerContainer).
		WithCommands([]string{driverInstallerCommand}).
		WithArgs([]string{driverInstallerInitArg}).
		WithEnvs(h.desiredSpec.Env).
		WithResources(h.desiredSpec.Resources, "250m", "40Mi").
		WithLifeCycle(&corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"/bin/sh", "-c", "rm -f " + validationsMountPath + "/.driver-ctr-ready"},
				},
			},
		}).
		WithSecurityContext(&corev1.SecurityContext{
			Privileged: ptr(true),
			RunAsUser:  ptr(int64(0)),
			SELinuxOptions: &corev1.SELinuxOptions{
				Level: "s0",
			},
		}).
		WithVolumeMounts([]corev1.VolumeMount{
			{
				Name:             hostDriverVolumeName,
				MountPath:        "/host/run/rbln/driver",
				MountPropagation: ptr(corev1.MountPropagationBidirectional),
			},
			{
				Name:      validationsVolumeName,
				MountPath: validationsMountPath,
			},
			{
				Name:      h.startupProbeConfigMapName(),
				MountPath: startupProbeScriptPath,
				SubPath:   startupProbeScriptName,
				ReadOnly:  true,
			},
		}).
		Build()

	if len(additionalVolumeMounts) > 0 {
		driverContainer.VolumeMounts = append(driverContainer.VolumeMounts, additionalVolumeMounts...)
	}

	driverContainer.StartupProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", startupProbeScriptPath},
			},
		},
		TimeoutSeconds:   driverManagerStartupProbeTimeoutSeconds,
		PeriodSeconds:    driverManagerStartupProbePeriodSeconds,
		FailureThreshold: driverManagerStartupProbeFailureThreshold,
	}

	driverSpec := *h.desiredSpec
	driverImagePath, err := driverSpec.GetPrecompiledImagePath(pool.getOS(), pool.kernel)
	if err != nil {
		return nil, err
	}
	driverContainer.Image = driverImagePath

	driverTag := fmt.Sprintf("%s-%s-%s", driverSpec.Version, pool.kernel, pool.getOS())
	driverPullPolicy := h.desiredSpec.ImagePullPolicy
	if driverPullPolicy == "" {
		driverPullPolicy = corev1.PullIfNotPresent
	}
	if driverTag == "latest" {
		driverPullPolicy = corev1.PullAlways
	}
	driverContainer.ImagePullPolicy = driverPullPolicy

	return driverContainer, nil
}

func (h *driverManagerPatcher) buildSubscriptionMountsAndVolumes(
	pool nodePool,
) ([]corev1.VolumeMount, []corev1.Volume, error) {
	subscriptionOS := ""
	if h.openshiftVersion != "" {
		subscriptionOS = "rhcos"
	} else if pool.osRelease == "rhel" {
		subscriptionOS = "rhel"
	}
	if subscriptionOS == "" {
		return nil, nil, nil
	}

	h.log.Info("Mounting subscription entitlements into driver container", "os", subscriptionOS, "nodePool", pool.name)
	pathToVolumeSource, err := getSubscriptionPathsToVolumeSources(subscriptionOS)
	if err != nil {
		return nil, nil, err
	}

	additionalVolumeMounts := make([]corev1.VolumeMount, 0, len(pathToVolumeSource))
	additionalVolumes := make([]corev1.Volume, 0, len(pathToVolumeSource))
	mountPaths := make([]string, 0, len(pathToVolumeSource))
	for mountPath := range pathToVolumeSource {
		mountPaths = append(mountPaths, mountPath)
	}
	sort.Strings(mountPaths)
	for i, mountPath := range mountPaths {
		volName := fmt.Sprintf("subscription-config-%d", i)
		additionalVolumeMounts = append(additionalVolumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
		additionalVolumes = append(additionalVolumes, corev1.Volume{
			Name:         volName,
			VolumeSource: pathToVolumeSource[mountPath],
		})
	}

	return additionalVolumeMounts, additionalVolumes, nil
}

func (h *driverManagerPatcher) buildDriverPodSpec(pool nodePool) (*corev1.PodSpec, error) {
	initContainer := h.buildDriverManagerInitContainer()

	additionalVolumeMounts, additionalVolumes, err := h.buildSubscriptionMountsAndVolumes(pool)
	if err != nil {
		return nil, err
	}

	driverContainer, err := h.buildDriverContainer(pool, additionalVolumeMounts)
	if err != nil {
		return nil, err
	}

	podSpec := k8sutils.NewPodSpecBuilder().
		WithServiceAccountName(h.name).
		WithNodeSelector(pool.nodeSelector).
		WithTolerations(h.desiredSpec.Tolerations).
		WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
		WithPriorityClassName(h.desiredSpec.PriorityClassName).
		WithVolumes([]corev1.Volume{
			{
				Name: hostDriverVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hostDriverPath,
						Type: ptr(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
			{
				Name: validationsVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: validationsMountPath,
						Type: ptr(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
			{
				Name: hostRootVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hostRootPath,
						Type: ptr(corev1.HostPathDirectory),
					},
				},
			},
			{
				Name: hostDevVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: hostDevPath,
						Type: ptr(corev1.HostPathDirectory),
					},
				},
			},
			{
				Name: h.startupProbeConfigMapName(),
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: h.startupProbeConfigMapName(),
						},
						Items: []corev1.KeyToPath{
							{
								Key:  startupProbeScriptName,
								Path: startupProbeScriptName,
								Mode: ptr(int32(0o755)),
							},
						},
					},
				},
			},
		}).
		WithInitContainers([]*corev1.Container{initContainer}).
		WithContainers([]*corev1.Container{driverContainer}).
		Build()

	if len(additionalVolumes) > 0 {
		podSpec.Volumes = append(podSpec.Volumes, additionalVolumes...)
	}

	return podSpec, nil
}

func (h *driverManagerPatcher) driverManagerLabels(pool nodePool) map[string]string {
	return map[string]string{
		driverManagerAppLabelKey:      h.name,
		driverManagerNodePoolLabelKey: pool.name,
		driverManagerInstanceLabelKey: h.instanceName,
	}
}

// getDaemonSet returns the current DaemonSet for this patcher namespace.
func (h *driverManagerPatcher) getDaemonSet(
	ctx context.Context,
	daemonSetName string,
) (*appsv1.DaemonSet, error) {
	current := &appsv1.DaemonSet{}
	err := h.client.Get(ctx, client.ObjectKey{
		Name:      daemonSetName,
		Namespace: h.namespace,
	}, current)
	if err != nil {
		return nil, err
	}
	return current, nil
}

func (h *driverManagerPatcher) shouldSkipDaemonSetUpdateByDriverHash(
	current *appsv1.DaemonSet,
	driverConfigDigest string,
) bool {
	if current == nil {
		return false
	}

	currentHash := current.Annotations[driverLastAppliedHashAnnotation]
	if currentHash == "" {
		currentHash = GetObjectHash(current.Spec.Template.Spec.Containers)
	}

	if currentHash == driverConfigDigest {
		h.log.Info(
			"Skip Driver Manager DaemonSet update: driver container unchanged",
			"namespace", current.Namespace,
			"name", current.Name,
			"hash", driverConfigDigest,
		)
		return true
	}

	return false
}

func upsertEnvVar(envs []corev1.EnvVar, target corev1.EnvVar) []corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == target.Name {
			envs[i] = target
			return envs
		}
	}
	return append(envs, target)
}
