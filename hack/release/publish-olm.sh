#!/usr/bin/env bash
# Publish dev/rc OLM artifacts from a previously resolved component image lock.
# Required env: VERSION, OLM_IMAGE_TAG, REGISTRY, HELM_CHART, HELM_CHART_VERSION.
# publish-olm.sh <component-digests.env> [output directory]
# Run after publishing the matching chart. GA uses certified-operators instead.
# On dev retries, verify existing SHA snapshots against the frozen inputs.
# EXPECTED_CHART_DIGEST binds a workflow retry to its original chart.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

digests=${1:?component-digests.env is required}
out=${2:-dist/olm}
: "${VERSION:?}" "${OLM_IMAGE_TAG:?}" "${REGISTRY:?}" "${HELM_CHART:?}" "${HELM_CHART_VERSION:?}"
if [ "$VERSION" = v0.0.0-dev ]; then
	kind=dev
elif [[ $VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+$ ]]; then
	kind=rc
else
	fail "only dev and rc OLM artifacts are published here (got $VERSION)"
fi

source_commit=$(git rev-parse HEAD)
if [ "$kind" = dev ]; then
	[ "$OLM_IMAGE_TAG" = "sha-$source_commit" ] || fail "dev OLM tag must be sha-$source_commit"
else
	[ "$OLM_IMAGE_TAG" = "$VERSION" ] || fail "rc OLM tag must match $VERSION"
fi
bundle_image="$REGISTRY/rbln-npu-operator-bundle:$OLM_IMAGE_TAG"
catalog_image="$REGISTRY/rbln-npu-operator-catalog:$OLM_IMAGE_TAG"
# Only a missing manifest permits a build. A failed authenticated lookup does
# not establish whether the previous attempt already pushed the snapshot.
snapshot_digest() {
	local image=$1 result
	if result=$(regctl image digest "$image" 2>&1); then
		[[ $result =~ ^sha256:[0-9a-f]{64}$ ]] || fail "invalid digest for $image: $result"
		echo "$result"
	else
		case "$result" in
			*"[http 404]"*|*"manifest unknown"*|*"MANIFEST_UNKNOWN"*) return 0 ;;
			*) fail "cannot check $image: $result" ;;
		esac
	fi
}
bundle_digest=$(snapshot_digest "$bundle_image")
catalog_digest=$(snapshot_digest "$catalog_image")
if [ "$kind" = rc ] && [ -n "$bundle_digest$catalog_digest" ]; then
	fail "RC snapshot already exists; refusing to overwrite a published OLM artifact"
fi
if [ -z "$bundle_digest" ] && [ -n "$catalog_digest" ]; then
	fail "catalog snapshot exists without its bundle: $catalog_image"
fi

pin() {
	local image=$1 digest
	digest=$(regctl image digest "$image")
	[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || fail "invalid digest for $image: $digest"
	printf '%s@%s\n' "$image" "$digest"
}
chart_image="${HELM_CHART#oci://}:$HELM_CHART_VERSION"
chart_ref=$(pin "$chart_image")
if [ -n "${EXPECTED_CHART_DIGEST:-}" ] && [ "$chart_ref" != "$chart_image@$EXPECTED_CHART_DIGEST" ]; then
	fail "$chart_image no longer matches the chart published for this run"
fi

bash hack/release/prepare-bundle.sh "$digests" "$VERSION" "$kind"
operator=$(awk -F= '$1 == "OPERATOR" {print $2}' "$digests")
validator=$(awk -F= '$1 == "VALIDATOR" {print $2}' "$digests")
csv=bundle/manifests/rbln-npu-operator.clusterserviceversion.yaml
csv_name=$(yq -er '.metadata.name' "$csv")
csv_version=$(yq -er '.spec.version' "$csv")

if [ -n "$bundle_digest$catalog_digest" ]; then
	remote_file=$(mktemp)
	trap 'rm -f "$remote_file"' EXIT
fi
if [ -n "$bundle_digest" ]; then
	bundle_ref="$bundle_image@$bundle_digest"
	# Verify the files copied by bundle.Dockerfile against the original inputs.
	# operator-sdk regenerates only the CSV createdAt timestamp on a retry.
	find bundle/manifests bundle/metadata bundle/tests/scorecard -type f -print0 |
	while IFS= read -r -d '' file; do
		regctl image get-file --platform linux/amd64 "$bundle_ref" "/${file#bundle/}" > "$remote_file"
		if [ "$file" = "$csv" ]; then
			expected=$(yq -o=json 'del(.metadata.annotations.createdAt)' "$file" | jq -cS .)
			actual=$(yq -o=json 'del(.metadata.annotations.createdAt)' "$remote_file" | jq -cS .)
			[ "$expected" = "$actual" ] || fail "published dev bundle differs from the requested inputs: $file"
		else
			cmp -s "$remote_file" "$file" || fail "published dev bundle differs from the requested inputs: $file"
		fi
	done
	note "Reusing verified dev bundle $bundle_ref"
else
	make build-bundle-image BUNDLE_IMAGE="$bundle_image" PLATFORM=linux/amd64 PUSH_ON_BUILD=true
	bundle_ref=$(pin "$bundle_image")
fi
make catalog VERSION="$VERSION" CATALOG_BUNDLE="$bundle_ref"
if [ -n "$catalog_digest" ]; then
	catalog_ref="$catalog_image@$catalog_digest"
	regctl image get-file --platform linux/amd64 "$catalog_ref" /configs/index.yaml > "$remote_file"
	cmp -s "$remote_file" catalog/index.yaml || fail "published dev catalog differs from the expected bundle $bundle_ref"
	note "Reusing verified dev catalog $catalog_ref"
else
	make catalog-build-image CATALOG_IMAGE="$catalog_image" PLATFORM=linux/amd64 PUSH_ON_BUILD=true
	catalog_ref=$(pin "$catalog_image")
fi

mkdir -p "$out"
jq -n \
	--arg kind "$kind" --arg commit "$source_commit" \
	--arg repository "${GITHUB_REPOSITORY:-RBLN-SW/rbln-npu-operator}" \
	--arg csv "$csv_name" --arg version "$csv_version" \
	--arg operator "$operator" --arg validator "$validator" \
	--arg bundle "$bundle_ref" --arg catalog "$catalog_ref" \
	--arg chart "$HELM_CHART" --arg chart_version "$HELM_CHART_VERSION" --arg chart_image "$chart_ref" \
	'{schema_version: 1, kind: $kind, source: {repository: $repository, commit: $commit},
	  package: "rbln-npu-operator", channel: "stable", csv: {name: $csv, version: $version},
	  images: {operator: $operator, validator: $validator, bundle: $bundle, catalog: $catalog},
	  chart: {ref: $chart, version: $chart_version, image: $chart_image,
	          source_path: "deployments/rbln-npu-operator"}}' > "$out/olm-artifacts.json"
cp "$digests" "$out/component-digests.env"

# Preserve the existing convenience aliases only after both snapshots exist.
# Catalog contents always refer to the immutable bundle digest, never :dev.
if [ "$kind" = dev ]; then
	[ "$(pin "$chart_image")" = "$chart_ref" ] || fail "dev chart moved during OLM publication"
	regctl image copy "$bundle_ref" "$REGISTRY/rbln-npu-operator-bundle:dev"
	regctl image copy "$catalog_ref" "$REGISTRY/rbln-npu-operator-catalog:dev"
fi
if [ -n "${GITHUB_OUTPUT:-}" ]; then
	echo "catalog_image=$catalog_ref" >> "$GITHUB_OUTPUT"
fi
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	{
		echo '### OLM test artifacts'
		echo '```json'
		cat "$out/olm-artifacts.json"
		echo '```'
	} >> "$GITHUB_STEP_SUMMARY"
fi
cat "$out/olm-artifacts.json"
