package drivermanager

import (
	"testing"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestResolveNPUPodEvictionPolicy(t *testing.T) {
	tests := map[string]struct {
		spec *rblnv1beta1.RBLNClusterPolicySpec
		want NPUPodEvictionPolicy
	}{
		// The strictest behaviour is the default: nothing is force-evicted and
		// no emptyDir contents are discarded without the user asking.
		"no cluster policy": {
			spec: nil,
			want: NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
		},
		"no upgrade policy block": {
			spec: &rblnv1beta1.RBLNClusterPolicySpec{},
			want: NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
		},
		"podDeletion is mirrored": {
			spec: &rblnv1beta1.RBLNClusterPolicySpec{
				Driver: rblnv1beta1.DriverSpec{UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{
					PodDeletion: &rblnv1beta1.PodDeletionSpec{Force: true, DeleteEmptyDirData: true},
				}},
			},
			want: NPUPodEvictionPolicy{
				Force: true, DeleteEmptyDirData: true, DeviceClass: consts.DefaultDRADeviceClass,
			},
		},
		// autoUpgrade is deliberately not consulted: k8s-driver-manager evicts
		// on its own only while auto-upgrade is off, so gating on it would make
		// the knobs unreachable in exactly the case they exist for.
		"podDeletion is mirrored even with autoUpgrade off": {
			spec: &rblnv1beta1.RBLNClusterPolicySpec{
				Driver: rblnv1beta1.DriverSpec{UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{
					AutoUpgrade: false,
					PodDeletion: &rblnv1beta1.PodDeletionSpec{DeleteEmptyDirData: true},
				}},
			},
			want: NPUPodEvictionPolicy{
				DeleteEmptyDirData: true, DeviceClass: consts.DefaultDRADeviceClass,
			},
		},
		"driverName overrides the device class": {
			spec: &rblnv1beta1.RBLNClusterPolicySpec{
				DRAKubeletPlugin: rblnv1beta1.RBLNDRAKubeletPluginSpec{DriverName: "npu.example.com"},
			},
			want: NPUPodEvictionPolicy{DeviceClass: "npu.example.com"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ResolveNPUPodEvictionPolicy(tc.spec); got != tc.want {
				t.Fatalf("ResolveNPUPodEvictionPolicy() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// Both k8s-driver-manager init containers, the driver pod's and the
// vfio-manager's, render exactly this list, so the contract with the binary
// lives in one place.
func TestNPUPodEvictionEnv(t *testing.T) {
	tests := map[string]struct {
		policy NPUPodEvictionPolicy
		want   map[string]string
	}{
		"defaults": {
			policy: NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
			want: map[string]string{
				"ENABLE_NPU_POD_EVICTION":               "true",
				"NPU_POD_EVICTION_FORCE":                "false",
				"NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA": "false",
				"NPU_POD_EVICTION_DEVICE_CLASS":         consts.DefaultDRADeviceClass,
			},
		},
		"relaxed policy with a custom device class": {
			policy: NPUPodEvictionPolicy{Force: true, DeleteEmptyDirData: true, DeviceClass: "npu.example.com"},
			want: map[string]string{
				"ENABLE_NPU_POD_EVICTION":               "true",
				"NPU_POD_EVICTION_FORCE":                "true",
				"NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA": "true",
				"NPU_POD_EVICTION_DEVICE_CLASS":         "npu.example.com",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := NPUPodEvictionEnv(tc.policy)
			if len(got) != len(tc.want) {
				t.Fatalf("NPUPodEvictionEnv() rendered %d vars, want %d: %+v", len(got), len(tc.want), got)
			}
			for _, env := range got {
				want, ok := tc.want[env.Name]
				if !ok {
					t.Errorf("unexpected env %q", env.Name)
					continue
				}
				if env.ValueFrom != nil {
					t.Errorf("env %q must be a literal value, got ValueFrom", env.Name)
				}
				if env.Value != want {
					t.Errorf("env %q = %q, want %q", env.Name, env.Value, want)
				}
			}
		})
	}
}
