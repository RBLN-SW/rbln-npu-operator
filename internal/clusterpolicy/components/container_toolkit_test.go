package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestContainerToolkitPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()

	name := consts.RBLNBaseName + "-" + consts.RBLNContainerToolkitName
	p := NewContainerToolkitPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "", consts.Containerd)

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.container-toolkit")

	if !ds.Spec.Template.Spec.HostPID {
		t.Fatal("expected HostPID=true")
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-container-toolkit", "v1.0")
	assertPrivileged(t, mainContainer)

	hasContainerdSocket := false
	for _, vol := range ds.Spec.Template.Spec.Volumes {
		if vol.Name == containerdSockVolumeName {
			hasContainerdSocket = true
			break
		}
	}
	if !hasContainerdSocket {
		t.Fatal("expected containerd socket volume")
	}

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container (driver-validation), got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	if ds.Spec.Template.Spec.InitContainers[0].Name != "driver-validation" {
		t.Fatalf("init container name = %q, want driver-validation", ds.Spec.Template.Spec.InitContainers[0].Name)
	}

	// Role (apps/daemonsets)
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "apps", "daemonsets")

	// RoleBinding
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)

	// ConfigMap (entrypoint)
	assertConfigMapHasKey(t, c, name+"-entrypoint", testNamespace, owner.Name, containerToolkitEntrypointKey)
}

func TestContainerToolkitSocketOverride(t *testing.T) {
	tests := map[string]struct {
		runtime        string
		envOverride    []corev1.EnvVar
		wantSocketPath string
		wantVolumeName string
	}{
		"containerd default socket": {
			runtime:        consts.Containerd,
			wantSocketPath: containerdSockPath,
			wantVolumeName: containerdSockVolumeName,
		},
		"containerd overridden socket": {
			runtime: consts.Containerd,
			envOverride: []corev1.EnvVar{
				{Name: "RBLN_CTK_DAEMON_SOCKET", Value: "/run/k3s/containerd/containerd.sock"},
			},
			wantSocketPath: "/run/k3s/containerd/containerd.sock",
			wantVolumeName: containerdSockVolumeName,
		},
		"docker default socket": {
			runtime:        consts.Docker,
			wantSocketPath: dockerSockPath,
			wantVolumeName: dockerSockVolumeName,
		},
		"docker overridden socket": {
			runtime: consts.Docker,
			envOverride: []corev1.EnvVar{
				{Name: "RBLN_CTK_DAEMON_SOCKET", Value: "/custom/docker.sock"},
			},
			wantSocketPath: "/custom/docker.sock",
			wantVolumeName: dockerSockVolumeName,
		},
		"crio default socket": {
			runtime:        consts.CRIO,
			wantSocketPath: crioSockPath,
			wantVolumeName: crioSockVolumeName,
		},
	}

	dsName := consts.RBLNBaseName + "-" + consts.RBLNContainerToolkitName

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := newFakeClient(t, scheme)

			owner := newTestOwner()
			owner.Spec.ContainerToolkit.Env = tc.envOverride

			p := NewContainerToolkitPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "", tc.runtime)
			if err := p.Patch(context.Background(), owner); err != nil {
				t.Fatalf("Patch() error: %v", err)
			}

			ds := &appsv1.DaemonSet{}
			assertObjectExists(t, c, types.NamespacedName{Name: dsName, Namespace: testNamespace}, ds)

			// Verify volume host path
			foundVolume := false
			for _, vol := range ds.Spec.Template.Spec.Volumes {
				if vol.Name == tc.wantVolumeName {
					foundVolume = true
					if vol.HostPath.Path != tc.wantSocketPath {
						t.Fatalf("volume %q host path = %q, want %q",
							tc.wantVolumeName, vol.HostPath.Path, tc.wantSocketPath)
					}
					break
				}
			}
			if !foundVolume {
				t.Fatalf("expected volume %q not found", tc.wantVolumeName)
			}

			// Verify volume mount path
			mainContainer := ds.Spec.Template.Spec.Containers[0]
			foundMount := false
			for _, vm := range mainContainer.VolumeMounts {
				if vm.Name == tc.wantVolumeName {
					foundMount = true
					if vm.MountPath != tc.wantSocketPath {
						t.Fatalf("volume mount %q path = %q, want %q",
							tc.wantVolumeName, vm.MountPath, tc.wantSocketPath)
					}
					break
				}
			}
			if !foundMount {
				t.Fatalf("expected volume mount %q not found", tc.wantVolumeName)
			}

			// Verify env var
			foundEnv := false
			for _, env := range mainContainer.Env {
				if env.Name == "RBLN_CTK_DAEMON_SOCKET" {
					foundEnv = true
					if env.Value != tc.wantSocketPath {
						t.Fatalf("env RBLN_CTK_DAEMON_SOCKET = %q, want %q",
							env.Value, tc.wantSocketPath)
					}
					break
				}
			}
			if !foundEnv {
				t.Fatalf("expected env RBLN_CTK_DAEMON_SOCKET not found")
			}
		})
	}
}

func TestContainerToolkitCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNContainerToolkitName
	p := NewContainerToolkitPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "", consts.Containerd)

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-entrypoint", Namespace: testNamespace}, &corev1.ConfigMap{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.RoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.Role{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
