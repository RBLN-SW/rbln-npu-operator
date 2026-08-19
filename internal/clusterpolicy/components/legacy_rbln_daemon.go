package components

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// legacyRBLNDaemonName is the pre-rename daemon component this controller
// used to own. Its successor, rbln-smd, is managed per-CR by the RBLNDriver
// reconciler; nothing garbage-collects the old objects because their owning
// RBLNClusterPolicy lives on.
// TODO(remove after two releases): delete this file, its registration in
// newComponents, and the smd patcher's pre-create delete once no supported
// upgrade path still ships the rbln-daemon objects.
const legacyRBLNDaemonName = "rbln-daemon"

// legacyRBLNDaemonPatcher is cleanup-only migration code: enabled is
// hardwired false so the PatchComponents loop runs CleanUp every reconcile —
// the same path any disabled component takes — deleting the pre-rename
// rbln-daemon objects.
type legacyRBLNDaemonPatcher struct {
	basePatcher
}

func NewLegacyRBLNDaemonCleanup(client client.Client, log logr.Logger, namespace string, scheme *runtime.Scheme, openshiftVersion string) Patcher {
	return &legacyRBLNDaemonPatcher{
		basePatcher: basePatcher{
			client:           client,
			log:              log,
			scheme:           scheme,
			name:             legacyRBLNDaemonName,
			namespace:        namespace,
			openshiftVersion: openshiftVersion,
			enabled:          false,
			workloadType:     consts.RBLNWorkloadConfigContainer,
		},
	}
}

func (h *legacyRBLNDaemonPatcher) Patch(_ context.Context, _ *rblnv1beta1.RBLNClusterPolicy) error {
	return nil
}

func (h *legacyRBLNDaemonPatcher) CleanUp(ctx context.Context, _ *rblnv1beta1.RBLNClusterPolicy) error {
	if err := h.deleteIfExists(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: h.name, Namespace: h.namespace},
	}); err != nil {
		return err
	}
	if err := h.deleteDaemonSet(ctx); err != nil {
		return err
	}
	if err := h.deleteOpenShiftRBAC(ctx); err != nil {
		return err
	}
	return h.deleteServiceAccount(ctx)
}
