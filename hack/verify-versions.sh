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

if [ "$fail" -ne 0 ]; then
	echo >&2
	echo "Version pin check failed. See docs/image-security.md for the compatibility" >&2
	echo "matrix and the order to apply dependency upgrades in." >&2
	exit 1
fi

echo "Version pin check completed."
