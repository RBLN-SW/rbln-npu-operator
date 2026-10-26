#!/usr/bin/env bash
# Manual entry point for the same RNPM RC path used by release.yaml.
# Requires RNPM_TOKEN, HARBOR_REGISTRY and the published SENTINEL_CATALOG_IMAGE.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export RC_TAG="${1:?usage: validate-rc.sh vX.Y.Z-rcN}"
export RC_SHA="${RC_SHA:-$(git rev-parse "$RC_TAG^{commit}")}"
export RC_BRANCH="${RC_BRANCH:-release-${RC_TAG#v}}"
export RC_BRANCH="${RC_BRANCH%-rc*}"
export GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-RBLN-SW/rbln-npu-operator}"
exec python3 .github/scripts/rnpm_rc.py
