#!/usr/bin/env bash
# Ask the npu-operator-test-infra matrix pipeline to validate a release candidate.
#
#   hack/release/validate-rc.sh <vX.Y.Z-rcN>
#
# Creates one build of the test-infra pipeline that installs the rc exactly as
# published (OCI chart from Harbor, rc operator/validator/catalog images, the
# operands the chart pins, no dev overrides) across the whole OS/runtime/
# platform matrix, and reports the verdict as the commit status `rc-validation`
# on the rc commit. classify-tag.sh / tag-release.sh read that status before GA.
#
# The pipeline runs on obedients, the in-house Buildkite-compatible CI. Two ways
# to create the build, chosen by what is configured:
#
#   trigger  BUILDKITE_TRIGGER_URL set: POST {branch, commit, message, env} to
#            the pipeline's webhook trigger. No token; the URL is the secret.
#            obedients applies the body's env (verified 2026-09-10, builds #82/#83),
#            unlike buildkite.com's triggers.
#   api      BUILDKITE_API_TOKEN + BUILDKITE_ORG + BUILDKITE_VALIDATE_PIPELINE
#            set: POST to the Buildkite REST Builds API with a bearer token.
#
# Builds still queued or running for an older rc of the same release are
# cancelled first (best effort) — GA only ever promotes the newest rc — but
# cancelling needs the REST API, so it only happens when the api settings are
# present. With the trigger alone, an older rc's build runs to completion (or
# the test-infra pipeline cancels it itself with its vault token).
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
#   BUILDKITE_TRIGGER_URL        webhook trigger URL of the test-infra pipeline
#   BUILDKITE_API_TOKEN          API token with read_builds + write_builds
#   BUILDKITE_ORG                organization slug (obedients: main)
#   BUILDKITE_VALIDATE_PIPELINE  pipeline slug (the test-infra matrix pipeline)
#   BUILDKITE_API_URL            REST base, default https://api.buildkite.com/v2
#                                (obedients: https://obedients-api.k8s.rebellions.in/v2)
#   BUILDKITE_VALIDATE_BRANCH    test-infra branch that holds the pipeline (default dev)
#   HARBOR_REGISTRY              default harbor.k8s.rebellions.in/rebellions
#   SENTINEL_REF                 optional passthrough
# With neither the trigger URL nor the api settings the script prints a notice
# and exits 0: a repository without them still publishes rcs, GA just cannot
# be gated on a status that nothing posts.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

tag=${1:?usage: validate-rc.sh <vX.Y.Z-rcN>}
[[ $tag =~ ^v([0-9]+\.[0-9]+\.[0-9]+)-rc[0-9]+$ ]] || fail "tag must be vX.Y.Z-rcN (got '$tag')"
version=${BASH_REMATCH[1]}
release="release-$version"

trigger=${BUILDKITE_TRIGGER_URL:-}
have_api=1
[ -n "${BUILDKITE_API_TOKEN:-}" ] && [ -n "${BUILDKITE_ORG:-}" ] && [ -n "${BUILDKITE_VALIDATE_PIPELINE:-}" ] || have_api=0
if [ -z "$trigger" ] && [ "$have_api" = 0 ]; then
	echo "::notice::rc validation not configured (secret BUILDKITE_TRIGGER_URL, or BUILDKITE_API_TOKEN + vars BUILDKITE_ORG / BUILDKITE_VALIDATE_PIPELINE). Skipping; no rc-validation status will be posted for $tag."
	exit 0
fi
command -v jq >/dev/null || fail "jq is required"

registry=${HARBOR_REGISTRY:-harbor.k8s.rebellions.in/rebellions}
repo=${GITHUB_REPOSITORY:-rebellions-sw/rbln-npu-operator}
branch=${BUILDKITE_VALIDATE_BRANCH:-dev}
sha=$(git rev-parse --verify "$tag^{commit}" 2>/dev/null) || fail "tag $tag is not available locally (fetch tags first)"
message="rc-validation $tag"
api=""
auth=()
if [ "$have_api" = 1 ]; then
	api="${BUILDKITE_API_URL:-https://api.buildkite.com/v2}/organizations/$BUILDKITE_ORG/pipelines/$BUILDKITE_VALIDATE_PIPELINE/builds"
	auth=(-H "Authorization: Bearer $BUILDKITE_API_TOKEN")
fi

# Cancel validations of older rcs of this release: only the newest rc can GA,
# so their verdict is moot. Identified by the build message prefix, which every
# server returns; a server without list or cancel support only costs a warning.
cancel_superseded() {
	local state n found=""
	for state in scheduled running; do
		found+=$(curl -sSf "${auth[@]}" "$api?branch=$branch&state=$state&per_page=50" 2>/dev/null |
			jq -r --arg p "rc-validation v$version-rc" \
				'.[] | select((.message // "") | startswith($p)) | .number' 2>/dev/null || true)
		found+=$'\n'
	done
	for n in $(sort -u <<<"$found"); do
		[ -n "$n" ] || continue
		if curl -sSf -X PUT "${auth[@]}" "$api/$n/cancel" >/dev/null 2>&1; then
			echo "cancelled superseded validation build #$n of $release"
		else
			echo "::warning::could not cancel build #$n (older rc of $release); it will run to completion"
		fi
	done
}
if [ "$have_api" = 1 ]; then
	cancel_superseded || echo "::warning::listing older validation builds failed; continuing"
else
	echo "::notice::no API settings; validations of older rcs of $release are not cancelled from here"
fi

env_json=$(jq -n \
	--arg chart "oci://$registry/rbln-npu-operator-chart" --arg ver "$version-${tag##*-}" \
	--arg catalog "$registry/rbln-npu-operator-catalog:$tag" \
	--arg repo "$repo" --arg sha "$sha" --arg sentinel_ref "${SENTINEL_REF:-}" \
	'{
	  RELEASE_MODE: "true", CLUSTERS: "all",
	  HELM_CHART: $chart, HELM_CHART_VERSION: $ver,
	  SENTINEL_CATALOG_IMAGE: $catalog,
	  RC_STATUS_REPO: $repo, RC_STATUS_SHA: $sha, RC_STATUS_CONTEXT: "rc-validation"
	} + (if $sentinel_ref == "" then {} else {SENTINEL_REF: $sentinel_ref} end)')
core=$(jq -n --arg branch "$branch" --arg msg "$message" --argjson env "$env_json" \
	'{commit: "HEAD", branch: $branch, message: $msg, env: $env}')

if [ -n "$trigger" ]; then
	# Webhook trigger: no auth header, the URL is the credential. obedients
	# honours branch/commit/message/env from the body.
	resp=$(curl -sSf -X POST -H 'Content-Type: application/json' -d "$core" "$trigger")
	url=$(jq -r '.build.web_url // .web_url // ("build #" + ((.build.number // .number)|tostring))' <<<"$resp")
	echo "rc-validation build for $tag (trigger): $url"
else
	# REST Builds API. Buildkite-only extras go on a first attempt and are
	# dropped if the server rejects them.
	full=$(jq --arg tag "$tag" --arg release "$release" --arg sha "$sha" \
		'. + {ignore_pipeline_branch_filters: true, meta_data: {rc_tag: $tag, release: $release, operator_commit: $sha}}' <<<"$core")
	post() { curl -sSf -X POST "${auth[@]}" -H 'Content-Type: application/json' -d "$1" "$api"; }
	if ! resp=$(post "$full" 2>/dev/null); then
		echo "::warning::build creation with meta_data/ignore_pipeline_branch_filters was rejected; retrying with the core payload"
		resp=$(post "$core")
	fi
	url=$(jq -r '.web_url // ("build #" + (.number|tostring))' <<<"$resp")
	echo "rc-validation build for $tag (api): $url"
fi
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	echo "rc-validation build for \`$tag\`: $url" >>"$GITHUB_STEP_SUMMARY"
fi
if [ -n "${GITHUB_OUTPUT:-}" ]; then
	echo "build_url=$url" >>"$GITHUB_OUTPUT"
fi
