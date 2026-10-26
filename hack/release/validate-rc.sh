#!/usr/bin/env bash
# Manual entry point for the same RNPM RC path used by release.yaml.
# Requires RNPM_TOKEN. Automated releases pass the pinned catalog image;
# manual calls default to the RC-tagged catalog in HARBOR_REGISTRY.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export RC_TAG="${1:?usage: validate-rc.sh vX.Y.Z-rcN}"
export RC_SHA="${RC_SHA:-$(git rev-parse "$RC_TAG^{commit}")}"
export RC_BRANCH="${RC_BRANCH:-release-${RC_TAG#v}}"
export RC_BRANCH="${RC_BRANCH%-rc*}"
export GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-RBLN-SW/rbln-npu-operator}"
export HARBOR_REGISTRY="${HARBOR_REGISTRY:-harbor.k8s.rebellions.in/rebellions}"
export SENTINEL_CATALOG_IMAGE="${SENTINEL_CATALOG_IMAGE:-${HARBOR_REGISTRY}/rbln-npu-operator-catalog:${RC_TAG}}"
exec python3 .github/scripts/rnpm_rc.py
