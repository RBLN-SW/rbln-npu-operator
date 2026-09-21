#!/usr/bin/env bash
#
# Checks that the version pins which must move together actually do.
#
# CVE remediation is the change most likely to introduce version skew here: a
# `go get` aimed at one advisory can leave the Kubernetes library family split
# across minors, or move controller-runtime without moving envtest. Both fail
# late and confusingly (a mismatched apiserver binary, or CRDs regenerating
# differently) rather than at the point of the mistake.
#
# See docs/image-security.md for the compatibility matrix and upgrade order.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail=0
err() {
	echo "  FAIL: $*" >&2
	fail=1
}
ok() { echo "  ok: $*"; }

# Version of a module as pinned in go.mod, empty if absent. Works for both
# direct and `// indirect` requires.
version_of() {
	awk -v m="$1" '$1 == m { print $2; exit }' go.mod
}

# Value of a `NAME ?= value` assignment in versions.mk.
mk_var() {
	grep "^$1" versions.mk | cut -d'=' -f2 | tr -d ' ?'
}

echo "Verifying version pins..."

# 1. Go toolchain. CI and the image build both read GOLANG_VERSION from
#    versions.mk, but a plain `docker build` with no --build-arg falls back to
#    the Dockerfile's ARG default -- which is exactly the path someone takes to
#    reproduce a scan finding by hand. If the two drift, a stdlib CVE fix looks
#    unapplied locally.
#
#    /.go-version is deliberately not checked: it is gitignored, so it is a
#    per-developer file that CI never sees.
mk_go="$(mk_var GOLANG_VERSION)"
df_go="$(grep -m1 '^ARG GOLANG_VERSION=' Dockerfile | cut -d'=' -f2)"
mod_go="$(grep -m1 '^go ' go.mod | awk '{print $2}')"

if [ "$mk_go" != "$df_go" ]; then
	err "GOLANG_VERSION: versions.mk has $mk_go but Dockerfile ARG default has $df_go"
else
	ok "Go toolchain $mk_go (versions.mk == Dockerfile ARG)"
fi

if [ "$(printf '%s\n%s\n' "$mod_go" "$mk_go" | sort -V | head -1)" != "$mod_go" ]; then
	err "go.mod requires go $mod_go, newer than the $mk_go toolchain builds use"
else
	ok "go.mod go directive $mod_go <= $mk_go"
fi

# 2. The Kubernetes library family must share one minor. Mixing them is the
#    classic way a targeted dependency bump breaks type registration.
#    klog, utils and kube-openapi are excluded: they do not follow the
#    v0.<k8s-minor>.<patch> scheme.
k8s_modules="
k8s.io/api
k8s.io/apimachinery
k8s.io/client-go
k8s.io/apiextensions-apiserver
k8s.io/apiserver
k8s.io/kubectl
k8s.io/component-base
k8s.io/cli-runtime
"
k8s_minor=""
k8s_ref=""
k8s_skew=0
for mod in $k8s_modules; do
	ver="$(version_of "$mod")"
	[ -n "$ver" ] || continue
	minor="$(echo "$ver" | cut -d. -f2)"
	if [ -z "$k8s_minor" ]; then
		k8s_minor="$minor"
		k8s_ref="$mod $ver"
	elif [ "$minor" != "$k8s_minor" ]; then
		err "k8s.io minor skew: $k8s_ref vs $mod $ver"
		k8s_skew=1
	fi
done
if [ -n "$k8s_minor" ] && [ "$k8s_skew" -eq 0 ]; then
	ok "k8s.io family all on v0.${k8s_minor}.x"
fi

# 3. controller-runtime and envtest are released in lockstep; ENVTEST_VERSION
#    tracks controller-runtime's release branch. Bumping one without the other
#    downloads test binaries for the wrong branch.
cr_ver="$(version_of sigs.k8s.io/controller-runtime)"
if [ -z "$cr_ver" ]; then
	err "sigs.k8s.io/controller-runtime not found in go.mod"
else
	cr_minor="${cr_ver#v}"
	cr_minor="$(echo "$cr_minor" | cut -d. -f1,2)"
	envtest="$(mk_var ENVTEST_VERSION)"
	expected="release-${cr_minor}"
	if [ "$envtest" != "$expected" ]; then
		err "controller-runtime $cr_ver expects ENVTEST_VERSION=$expected, versions.mk has $envtest"
	else
		ok "controller-runtime $cr_ver paired with ENVTEST_VERSION=$envtest"
	fi
fi

# 4. rbln-k8s-driver-manager ships as a version pair with this operator (see
#    the README, "version pair"), and the operator pins it in two places that
#    must agree: the driver pod's init container (driver.manager) and the
#    vfio-manager's (vfioManager.driverManager). Both render the same
#    NPU_POD_EVICTION_* contract, so a pin left behind runs a binary that
#    ignores it. The chart, its sample overlays, the CR samples and the
#    RELATED_IMAGE in config/manager all carry the pin by hand.
dm_files="
deployments/rbln-npu-operator/values.yaml
deployments/rbln-npu-operator/sample-values-ContainerWorkload.yaml
deployments/rbln-npu-operator/sample-values-SandboxWorkload.yaml
config/samples/v1beta1_rblnclusterpolicy.yaml
config/samples/v1alpha1_rblndriver.yaml
"
dm_ref=""
dm_count=0
dm_skew=0
check_dm_pin() {
	local file="$1" ver="$2"
	if [ -z "$ver" ]; then
		err "rbln-k8s-driver-manager: no version pin found in $file"
		dm_skew=1
		return
	fi
	dm_count=$((dm_count + 1))
	if [ -z "$dm_ref" ]; then
		dm_ref="$ver"
	elif [ "$ver" != "$dm_ref" ]; then
		err "rbln-k8s-driver-manager pin skew: $file has $ver, expected $dm_ref"
		dm_skew=1
	fi
}
for f in $dm_files; do
	# The pin is the tag: or version: line that follows the image name.
	for ver in $(awk '/rbln-k8s-driver-manager$/ { getline; if ($1 == "tag:" || $1 == "version:") print $2 }' "$f"); do
		check_dm_pin "$f" "$ver"
	done
done
check_dm_pin config/manager/manager.yaml \
	"$(grep -o 'rbln-k8s-driver-manager:v[^@ ]*' config/manager/manager.yaml | head -1 | cut -d: -f2)"
if [ "$dm_skew" -eq 0 ] && [ "$dm_count" -gt 0 ]; then
	ok "rbln-k8s-driver-manager pinned at $dm_ref in $dm_count places"
fi

# 5. The pair is also a floor. The NPU-only eviction contract -- the
#    NPU_POD_EVICTION_* env, DeviceClass-keyed claim eviction and the cordon
#    claim -- is bound from rbln-k8s-driver-manager v0.3.0 (see
#    docs/driver-upgrade.md). An older pin runs a binary that drains the whole
#    node on the driver path and evicts nothing on the vfio path, and agreeing
#    pins do not catch that. Pre-releases of the floor version count as
#    meeting it: sort -V orders v0.3.0-rc1 after v0.3.0.
dm_floor="v0.3.0"
if [ "$dm_skew" -eq 0 ] && [ -n "$dm_ref" ]; then
	if [ "$(printf '%s\n%s\n' "$dm_floor" "$dm_ref" | sort -V | head -1)" != "$dm_floor" ]; then
		err "rbln-k8s-driver-manager pin $dm_ref is below the $dm_floor floor the NPU-only eviction contract requires"
	else
		ok "rbln-k8s-driver-manager pin $dm_ref meets the $dm_floor floor"
	fi
fi

if [ "$fail" -ne 0 ]; then
	echo >&2
	echo "Version pin check failed. See docs/image-security.md for the compatibility" >&2
	echo "matrix and the order to apply dependency upgrades in." >&2
	exit 1
fi

echo "Version pin check completed."
