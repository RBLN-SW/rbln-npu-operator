/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package components

import (
	"context"
	"errors"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/registry"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func TestProbeMigrationPreservesOldTemplates(t *testing.T) {
	const creationDeferred = "creation deferred"
	for _, scenario := range []string{"updated", "missing family", "missing image", "update failed", creationDeferred} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			h := newTestPatcher(t, "")
			owner := newTestOwner()
			node := familyNode("worker", "atom")
			pool := nodePool{name: familyPoolName("atom"), osRelease: "ubuntu", osVersion: "22.04", kernel: "5.15.0-100-generic", family: "atom"}
			image, err := h.desiredSpec.GetPrecompiledImagePath(pool.getOS(), pool.kernel, pool.family)
			if err != nil {
				t.Fatal(err)
			}
			h.checker = &fakeChecker{}
			if scenario == "missing image" {
				h.checker = &fakeChecker{verdicts: map[string]registry.Verdict{image: registry.VerdictNotFound}}
			}
			if scenario == "missing family" {
				delete(node.Labels, consts.RBLNNPUFamilyLabelKey)
			}
			h.ownedNodes = []corev1.Node{*node}
			if scenario == creationDeferred {
				unlabeled := familyNode("unlabeled", "atom")
				delete(unlabeled.Labels, consts.RBLNNPUFamilyLabelKey)
				h.ownedNodes = append(h.ownedNodes, *unlabeled)
			}

			legacyName := driverManagerName + "-" + startupProbeConfigMapSuffix
			legacyData := map[string]string{startupProbeScriptName: "#!/bin/sh\n# legacy probe has no entrypoint-start marker requirement\nexit 0\n"}
			legacyCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: legacyName, Namespace: testNamespace}, Data: legacyData}
			oldSpec := h.buildDriverPodSpec(pool, image)
			oldSpec.Containers[0].Command = []string{driverInstallerCommand}
			oldSpec.Containers[0].Args = []string{driverInstallerInitArg}
			for i := range oldSpec.Volumes {
				if oldSpec.Volumes[i].ConfigMap != nil {
					oldSpec.Volumes[i].Name = legacyName
					oldSpec.Volumes[i].ConfigMap.Name = legacyName
				}
			}
			for i := range oldSpec.Containers[0].VolumeMounts {
				if oldSpec.Containers[0].VolumeMounts[i].MountPath == startupProbeScriptPath {
					oldSpec.Containers[0].VolumeMounts[i].Name = legacyName
				}
			}
			oldDS := k8sutil.NewDaemonSetBuilder(h.instanceName+"-"+pool.name, h.namespace).
				WithLabels(h.driverManagerLabels(pool)).WithLabelSelectors(h.driverManagerLabels(pool)).
				WithPodSpec(oldSpec).WithUpdateStrategy(appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}).Build()
			if scenario == creationDeferred {
				oldDS.Name = "legacy-pool"
				oldDS.Labels[driverManagerNodePoolLabelKey] = "legacy-pool"
			}
			oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: testNamespace}, Spec: *oldSpec.DeepCopy()}
			updateErr := errors.New("DaemonSet update failed")
			c := fake.NewClientBuilder().WithScheme(h.scheme).WithObjects(legacyCM, oldDS, oldPod).
				WithInterceptorFuncs(interceptor.Funcs{
					Update: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.UpdateOption) error {
						if _, isDS := object.(*appsv1.DaemonSet); isDS && scenario == "update failed" {
							return updateErr
						}
						return c.Update(ctx, object, opts...)
					},
				}).Build()
			h.client, h.apiReader = c, c
			err = h.Patch(ctx, owner)
			if scenario == "update failed" {
				if !errors.Is(err, updateErr) {
					t.Fatalf("Patch() = %v, want injected update failure", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}

			retainedCM := &corev1.ConfigMap{}
			assertObjectExists(t, c, client.ObjectKeyFromObject(legacyCM), retainedCM)
			if !reflect.DeepEqual(retainedCM.Data, legacyData) {
				t.Fatal("old templates would receive the new, incompatible probe")
			}
			newCM := &corev1.ConfigMap{}
			assertObjectExists(t, c, client.ObjectKey{Namespace: testNamespace, Name: h.startupProbeConfigMapName()}, newCM)
			if newCM.Name == legacyName || newCM.Immutable == nil || !*newCM.Immutable {
				t.Fatal("new probe must have a distinct immutable revision")
			}
			retainedPod := &corev1.Pod{}
			assertObjectExists(t, c, client.ObjectKeyFromObject(oldPod), retainedPod)
			assertProbeReference(t, &retainedPod.Spec, legacyName)
			current := &appsv1.DaemonSet{}
			assertObjectExists(t, c, client.ObjectKeyFromObject(oldDS), current)
			if scenario == "updated" {
				assertProbeReference(t, &current.Spec.Template.Spec, newCM.Name)
				if !reflect.DeepEqual(current.Spec.Template.Spec.Containers[0].Command, []string{"/bin/sh", "-ec"}) {
					t.Fatal("new probe must be paired with the entrypoint wrapper")
				}
			} else {
				assertProbeReference(t, &current.Spec.Template.Spec, legacyName)
			}
			if scenario == creationDeferred {
				assertObjectNotExists(t, c, client.ObjectKey{Namespace: testNamespace, Name: h.instanceName + "-" + pool.name}, &appsv1.DaemonSet{})
			}
		})
	}
}

func TestProbeRevisionDataAndSharedOwners(t *testing.T) {
	ctx := context.Background()
	h := newTestPatcher(t, "")
	oldData := map[string]string{startupProbeScriptName: "old probe"}
	oldRevision := driverManagerName + "-" + startupProbeConfigMapSuffix + "-" + k8sutil.GetObjectHash(oldData)[:16]
	oldCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: oldRevision, Namespace: testNamespace},
		Data:       oldData,
		Immutable:  ptr(true),
	}
	if err := h.client.Create(ctx, oldCM); err != nil {
		t.Fatal(err)
	}
	first, second := newTestOwner(), newTestOwner()
	second.Name, second.UID = "other-driver", "other-uid"
	for _, owner := range []*rebellionsaiv1alpha1.RBLNDriver{first, second, first} {
		if err := h.handleConfigMap(ctx, owner); err != nil {
			t.Fatal(err)
		}
	}
	current := &corev1.ConfigMap{}
	assertObjectExists(t, h.client, client.ObjectKey{Name: h.startupProbeConfigMapName(), Namespace: testNamespace}, current)
	want := driverManagerName + "-" + startupProbeConfigMapSuffix + "-" + k8sutil.GetObjectHash(current.Data)[:16]
	if current.Name != want || current.Name == oldRevision || len(current.Name) > 63 {
		t.Fatalf("probe name must identify its data and fit a volume name: %q, want %q", current.Name, want)
	}
	if len(current.OwnerReferences) != 2 {
		t.Fatalf("shared revision lost or duplicated driver owners: %+v", current.OwnerReferences)
	}
	retained := &corev1.ConfigMap{}
	assertObjectExists(t, h.client, client.ObjectKeyFromObject(oldCM), retained)
	if !reflect.DeepEqual(retained.Data, oldData) {
		t.Fatal("publishing a new revision changed the old immutable probe")
	}
}

func TestProbeRevisionDoesNotRenameContainerMount(t *testing.T) {
	h := newTestPatcher(t, "")
	spec := h.buildDriverPodSpec(nodePool{}, "driver:test")
	assertProbeReference(t, spec, h.startupProbeConfigMapName())
	stableName := driverManagerName + "-" + startupProbeConfigMapSuffix
	for _, mount := range spec.Containers[0].VolumeMounts {
		if mount.MountPath == startupProbeScriptPath && mount.Name != stableName {
			t.Fatalf("probe content leaked into the driver container's mount name: %q", mount.Name)
		}
	}
	oldDigest, oldTemplate := k8sutil.GetObjectHash(spec.Containers), k8sutil.GetObjectHash(spec)
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == stableName {
			spec.Volumes[i].ConfigMap.Name = stableName + "-next-revision"
		}
	}
	if k8sutil.GetObjectHash(spec.Containers) != oldDigest || k8sutil.GetObjectHash(spec) == oldTemplate {
		t.Fatal("a probe-only revision must change the Pod template hash but preserve the driver config digest")
	}
}

func assertProbeReference(t *testing.T, spec *corev1.PodSpec, want string) {
	t.Helper()
	for _, mount := range spec.Containers[0].VolumeMounts {
		if mount.MountPath != startupProbeScriptPath {
			continue
		}
		for _, volume := range spec.Volumes {
			if volume.Name == mount.Name && volume.ConfigMap != nil && volume.ConfigMap.Name == want {
				return
			}
		}
	}
	t.Fatalf("Pod template does not bind the expected probe revision %q", want)
}
