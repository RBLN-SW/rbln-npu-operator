#!/usr/bin/env bash
# Ask the npu-operator-test-infra matrix pipeline to validate a release candidate.
#
#   hack/release/validate-rc.sh <vX.Y.Z-rcN>
#
# Creates one Buildkite build that installs the rc exactly as published (OCI
# chart from Harbor, rc operator/validator/catalog images, the operands the
# chart pins, no dev overrides) across the whole OS/runtime/platform matrix,
# and reports the verdict as the commit status `rc-validation` on the rc
# commit. classify-tag.sh / tag-release.sh read that status before GA.
# Builds still queued or running for an older rc of the same release are
# cancelled first: GA only ever promotes the newest rc, so their verdict is moot.
#
# Contract with the test-infra pipeline (build env; RELEASE_MODE turns off every
# dev image override there):
#   RELEASE_MODE=true  CLUSTERS=all
#   HELM_CHART / HELM_CHART_VERSION      OCI chart to install as-is
#   SENTINEL_CATALOG_IMAGE               rc FBC catalog image for the OLM scenario
#   RC_STATUS_REPO / RC_STATUS_SHA / RC_STATUS_CONTEXT
#                                        where the final aggregation step posts
#                                        success | failure | error (error =
#                                        preflight/infrastructure, not the rc)
#   SENTINEL_REF                         optional, pins the harness for the run
#
# Environment:
#   BUILDKITE_API_TOKEN          token with read_builds + write_builds
#   BUILDKITE_ORG                organization slug
#   BUILDKITE_VALIDATE_PIPELINE  pipeline slug (the test-infra matrix pipeline)
#   BUILDKITE_VALIDATE_BRANCH    test-infra branch that holds the pipeline (default dev)
#   HARBOR_REGISTRY              default harbor.k8s.rebellions.in/rebellions
#   SENTINEL_REF                 optional passthrough
# When the three Buildkite settings are absent the script prints a notice and
# exits 0: a repository without them still publishes rcs, GA just cannot be
# gated on a status that nothing posts.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

tag=${1:?usage: validate-rc.sh <vX.Y.Z-rcN>}
[[ $tag =~ ^v([0-9]+\.[0-9]+\.[0-9]+)-rc[0-9]+$ ]] || fail "tag must be vX.Y.Z-rcN (got '$tag')"
version=${BASH_REMATCH[1]}
release="release-$version"

if [ -z "${BUILDKITE_API_TOKEN:-}" ] || [ -z "${BUILDKITE_ORG:-}" ] || [ -z "${BUILDKITE_VALIDATE_PIPELINE:-}" ]; then
	echo "::notice::rc validation not configured (secret BUILDKITE_API_TOKEN, vars BUILDKITE_ORG / BUILDKITE_VALIDATE_PIPELINE). Skipping; no rc-validation status will be posted for $tag."
	exit 0
fi
command -v jq >/dev/null || fail "jq is required"

registry=${HARBOR_REGISTRY:-harbor.k8s.rebellions.in/rebellions}
repo=${GITHUB_REPOSITORY:-rebellions-sw/rbln-npu-operator}
sha=$(git rev-parse --verify "$tag^{commit}" 2>/dev/null) || fail "tag $tag is not available locally (fetch tags first)"
api="https://api.buildkite.com/v2/organizations/$BUILDKITE_ORG/pipelines/$BUILDKITE_VALIDATE_PIPELINE/builds"
auth=(-H "Authorization: Bearer $BUILDKITE_API_TOKEN")

# Cancel validations of older rcs of this release; only the newest rc can GA.
for state in scheduled running; do
	curl -sSf "${auth[@]}" "$api?state=$state&meta_data%5Brelease%5D=$release&per_page=50" | jq -r '.[].number'
done | sort -u | while read -r n; do
	[ -n "$n" ] || continue
	curl -sSf -X PUT "${auth[@]}" "$api/$n/cancel" >/dev/null &&
		echo "cancelled superseded validation build #$n of $release"
done

body=$(jq -n \
	--arg branch "${BUILDKITE_VALIDATE_BRANCH:-dev}" \
	--arg msg "rc-validation $tag" \
	--arg tag "$tag" --arg release "$release" --arg sha "$sha" --arg repo "$repo" \
	--arg chart "oci://$registry/rbln-npu-operator-chart" --arg ver "$version-${tag##*-}" \
	--arg catalog "$registry/rbln-npu-operator-catalog:$tag" \
	--arg sentinel_ref "${SENTINEL_REF:-}" \
	'{
	  commit: "HEAD", branch: $branch, message: $msg,
	  ignore_pipeline_branch_filters: true,
	  meta_data: {rc_tag: $tag, release: $release, operator_commit: $sha},
	  env: ({
	    RELEASE_MODE: "true", CLUSTERS: "all",
	    HELM_CHART: $chart, HELM_CHART_VERSION: $ver,
	    SENTINEL_CATALOG_IMAGE: $catalog,
	    RC_STATUS_REPO: $repo, RC_STATUS_SHA: $sha, RC_STATUS_CONTEXT: "rc-validation"
	  } + (if $sentinel_ref == "" then {} else {SENTINEL_REF: $sentinel_ref} end))
	}')

resp=$(curl -sSf -X POST "${auth[@]}" -H 'Content-Type: application/json' -d "$body" "$api")
url=$(jq -r '.web_url' <<<"$resp")
echo "rc-validation build for $tag: $url"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	echo "rc-validation build for \`$tag\`: $url" >>"$GITHUB_STEP_SUMMARY"
fi
