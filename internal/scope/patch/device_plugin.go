package patch

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const (
	defaultHostBinPath = "/usr/bin"
	hostBinVolumeName  = "host-bin"
	rblnSMIBinaryName  = "rbln-smi"
)

type devicePluginPatcher struct {
	client client.Client
	log    logr.Logger
	scheme *runtime.Scheme

	desiredSpec      *rblnv1beta1.RBLNDevicePluginSpec
	name             string
	namespace        string
	openshiftVersion string
}

func NewDevicePluginPatcher(client client.Client, log logr.Logger, namespace string, cpSpec *rblnv1beta1.RBLNClusterPolicySpec, scheme *runtime.Scheme, openshiftVersion string) (Patcher, error) {
	patcher := &devicePluginPatcher{
		client: client,
		log:    log,
		scheme: scheme,

		name:             cpSpec.BaseName + "-" + consts.RBLNDevicePluginName,
		namespace:        namespace,
		openshiftVersion: openshiftVersion,
	}

	synced := syncSpec(cpSpec, cpSpec.DevicePlugin)
	patcher.desiredSpec = &synced
	return patcher, nil
}

func (h *devicePluginPatcher) IsEnabled() bool {
	if h.desiredSpec == nil {
		return false
	}

	return h.desiredSpec.IsEnabled()
}

func (h *devicePluginPatcher) Patch(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	if !h.desiredSpec.IsEnabled() {
		return nil
	}

	// reconcile serviceaccount
	if err := h.handleServiceAccount(ctx, owner); err != nil {
		return err
	}

	if h.openshiftVersion != "" {
		if err := h.handleRole(ctx, owner); err != nil {
			return err
		}
		if err := h.handleRoleBinding(ctx, owner); err != nil {
			return err
		}
	}

	// reconcile configmap
	if err := h.handleConfigMap(ctx, owner); err != nil {
		return err
	}

	// reconcile daemonset
	if err := h.handleDaemonSet(ctx, owner); err != nil {
		return err
	}

	return nil
}

func (h *devicePluginPatcher) CleanUp(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	h.log.Info("WARNING: Device Plugin is disabled. Remove all Device Plugin resources")
	if err := h.client.Delete(ctx, &v1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name,
			Namespace: h.namespace,
		},
	}); err != nil && !kapierrors.IsNotFound(err) {
		return err
	}
	if err := h.client.Delete(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.name + "-config",
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

func (h *devicePluginPatcher) ConditionReport(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) ([]metav1.Condition, error) {
	var ds v1.DaemonSet
	if err := h.client.Get(ctx, types.NamespacedName{Name: h.name, Namespace: h.namespace}, &ds); err != nil {
		return []metav1.Condition{{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionFalse,
			Reason:             DaemonSetNotFound,
			Message:            fmt.Sprintf("DaemonSet %s/%s could not be found: %v", h.namespace, h.name, err),
			LastTransitionTime: metav1.Now(),
		}}, nil
	}

	ready := ds.Status.DesiredNumberScheduled > 0 &&
		ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberUnavailable == 0

	observedGen := ds.GetGeneration()

	if !ready {
		return []metav1.Condition{
			{
				Type:   DaemonSetReady,
				Status: metav1.ConditionFalse,
				Reason: DaemonSetPodsNotReady,
				Message: fmt.Sprintf(
					"DaemonSet %s/%s is progressing: %d of %d pods are Ready (%d unavailable)",
					h.namespace,
					h.name,
					ds.Status.NumberReady,
					ds.Status.DesiredNumberScheduled,
					ds.Status.NumberUnavailable,
				),
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: observedGen,
			},
		}, nil
	}

	return []metav1.Condition{
		{
			Type:               DaemonSetReady,
			Status:             metav1.ConditionTrue,
			Reason:             DaemonSetAllPodsReady,
			Message:            fmt.Sprintf("All pods in DaemonSet %s/%s are running", h.namespace, h.name),
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: observedGen,
		},
	}, nil
}

func (h *devicePluginPatcher) ComponentName() string {
	return h.name
}

func (h *devicePluginPatcher) ComponentNamespace() string {
	return h.namespace
}

func (h *devicePluginPatcher) getHostBinPath() string {
	if h.desiredSpec != nil && h.desiredSpec.HostBinPath != "" {
		return h.desiredSpec.HostBinPath
	}
	return defaultHostBinPath
}

func (h *devicePluginPatcher) handleServiceAccount(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewServiceAccountBuilder(h.name, h.namespace)
	sa := builder.Build()

	saRes, err := controllerutil.CreateOrPatch(ctx, h.client, sa, func() error {
		sa = builder.WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDevicePlugin ServiceAccount")
		return err
	}
	h.log.Info("Reconciled RBLNDevicePlugin ServiceAccount", "namespace", sa.Namespace, "name", sa.Name, "result", saRes)
	return nil
}

func (h *devicePluginPatcher) handleRole(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBuilder(h.name, h.namespace)
	rb := builder.Build()

	roleRes, err := controllerutil.CreateOrPatch(ctx, h.client, rb, func() error {
		rb = builder.
			WithRules(rbacv1.PolicyRule{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"privileged"},
				Verbs:         []string{"use"},
			}).
			WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDevicePlugin Role")
		return err
	}
	h.log.Info("Reconciled RBLNDevicePlugin Role", "namespace", rb.Namespace, "name", rb.Name, "result", roleRes)
	return nil
}

func (h *devicePluginPatcher) handleRoleBinding(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewRoleBindingBuilder(h.name, h.namespace)
	rbb := builder.Build()

	roleBindingRes, err := controllerutil.CreateOrPatch(ctx, h.client, rbb, func() error {
		rbb = builder.
			WithRoleRef(rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     h.name,
			}).
			WithSubjects(rbacv1.Subject{
				Kind:      "ServiceAccount",
				Name:      h.name,
				Namespace: h.namespace,
			}).
			WithOwner(owner, h.scheme).Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDevicePlugin RoleBinding")
		return err
	}
	h.log.Info("Reconciled RBLNDevicePlugin RoleBinding", "namespace", rbb.Namespace, "name", rbb.Name, "result", roleBindingRes)
	return nil
}

func (h *devicePluginPatcher) buildDevicePluginConfig() (string, error) {
	configResources := make([]configResource, 0)

	for _, resource := range h.desiredSpec.ResourceList {

		devices, err := collectDevices(resource.ProductCardNames)
		if err != nil {
			h.log.Error(err, "Failed to collect devices for resource", "resourceName", resource.ResourceName)
			return "", err
		}

		configResources = append(configResources, configResource{
			ResourceName:   resource.ResourceName,
			ResourcePrefix: resource.ResourcePrefix,
			DeviceType:     consts.DeviceTypeAccelerator,
			Selectors: deviceSelector{
				Vendors: []string{consts.RBLNVendorCode},
				Drivers: []string{consts.RBLNDriverName},
				Devices: devices,
			},
		})
	}

	configFile := configResourceList{
		ResourceList: configResources,
	}
	configDataBytes, err := json.MarshalIndent(configFile, "", "  ")
	if err != nil {
		h.log.Error(err, "Failed to marshal device plugin config")
		return "", err
	}

	return string(configDataBytes), nil
}

func (h *devicePluginPatcher) handleConfigMap(ctx context.Context, cp *rblnv1beta1.RBLNClusterPolicy) error {
	builder := k8sutil.NewConfigMapBuilder(h.name+"-config", h.namespace)
	cm := builder.Build()

	configData, err := h.buildDevicePluginConfig()
	if err != nil {
		h.log.Error(err, "Failed to build device plugin config")
		return err
	}

	cmRes, err := controllerutil.CreateOrPatch(ctx, h.client, cm, func() error {
		cm = builder.
			WithData(map[string]string{
				"config.json": configData,
			}).
			WithOwner(cp, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDevicePlugin ConfigMap")
		return err
	}

	h.log.Info("Reconciled RBLNDevicePlugin ConfigMap", "namespace", cm.Namespace, "name", cm.Name, "result", cmRes)
	return nil
}

func (h *devicePluginPatcher) handleDaemonSet(ctx context.Context, owner *rblnv1beta1.RBLNClusterPolicy) error {
	hostRblnSMIPath := filepath.Join(h.getHostBinPath(), rblnSMIBinaryName)

	builder := k8sutil.NewDaemonSetBuilder(h.name, h.namespace)
	ds := builder.Build()
	dsRes, err := controllerutil.CreateOrPatch(ctx, h.client, ds, func() error {
		ds = builder.
			WithLabelSelectors(map[string]string{"app": h.name}).
			WithLabels(h.desiredSpec.Labels).
			WithAnnotations(h.desiredSpec.Annotations).
			WithPodSpec(k8sutil.NewPodSpecBuilder().
				WithServiceAccountName(h.name).
				WithNodeSelector(map[string]string{"rebellions.ai/npu.deploy.device-plugin": "true"}).
				WithAffinity(h.desiredSpec.Affinity).
				WithTolerations(h.desiredSpec.Tolerations).
				WithImagePullSecrets(h.desiredSpec.ImagePullSecrets).
				WithVolumes([]corev1.Volume{
					{
						Name: "devicesock",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/lib/kubelet/device-plugin",
							},
						},
					},
					{
						Name: "plugins-registry",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/lib/kubelet/plugins_registry",
							},
						},
					},
					{
						Name: "log",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/log",
							},
						},
					},
					{
						Name: "device-info",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/var/run/k8s.cni.cncf.io/devinfo/dp",
								Type: &[]corev1.HostPathType{"DirectoryOrCreate"}[0],
							},
						},
					},
					{
						Name: "config-volume",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: h.name + "-config",
								},
								Items: []corev1.KeyToPath{
									{
										Key:  "config.json",
										Path: "config.json",
									},
								},
							},
						},
					},
					{
						Name: "host-sys",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/sys",
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
					{
						Name: "host-dev",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/dev",
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
					{
						Name: hostBinVolumeName,
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: h.getHostBinPath(),
								Type: ptr(corev1.HostPathDirectory),
							},
						},
					},
				}).
				WithContainers([]*corev1.Container{
					k8sutil.NewContainerBuilder().
						WithName(h.name).
						WithImage(h.desiredSpec.Image, h.desiredSpec.Version, h.desiredSpec.ImagePullPolicy).
						WithResources(h.desiredSpec.Resources, "250m", "40Mi").
						WithVolumeMounts([]corev1.VolumeMount{
							{
								Name:      "devicesock",
								MountPath: "/var/lib/kubelet/device-plugins",
								ReadOnly:  false,
							},
							{
								Name:      "plugins-registry",
								MountPath: "/var/lib/kubelet/plugins_registry",
								ReadOnly:  false,
							},
							{
								Name:      "log",
								MountPath: "/var/log",
							},
							{
								Name:      "device-info",
								MountPath: "/var/run/k8s.cni.cncf.io/devinfo/dp",
							},
							{
								Name:      "config-volume",
								MountPath: "/etc/pcidp",
							},
							{
								Name:      hostBinVolumeName,
								MountPath: hostRblnSMIPath,
								SubPath:   rblnSMIBinaryName,
								ReadOnly:  true,
							},
							{
								Name:      "host-dev",
								MountPath: "/dev",
							},
							{
								Name:      "host-sys",
								MountPath: "/sys",
							},
						}).
						WithSecurityContext(&corev1.SecurityContext{
							Privileged: ptr(true),
							RunAsUser:  ptr(int64(0)),
						}).
						Build(),
				}).
				Build(),
			).
			WithOwner(owner, h.scheme).
			Build()
		return nil
	})
	if err != nil {
		h.log.Error(err, "Failed to reconcile RBLNDevicePlugin DaemonSet")
		return err
	}

	h.log.Info("Reconciled RBLNDevicePlugin DaemonSet", "namespace", ds.Namespace, "name", ds.Name, "result", dsRes)
	return nil
}
