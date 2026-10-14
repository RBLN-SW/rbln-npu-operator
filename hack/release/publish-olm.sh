#!/usr/bin/env bash
# Publish dev/rc OLM artifacts from a previously resolved component image lock.
# Required env: VERSION, OLM_IMAGE_TAG, REGISTRY, HELM_CHART, HELM_CHART_VERSION.
# publish-olm.sh <component-digests.env> [output directory]
# Run after publishing the matching chart. GA uses certified-operators instead.
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
# These are versioned snapshots, never moving aliases. Refuse a full rerun
# which would silently replace artifacts that another organization may test.
for image in "$bundle_image" "$catalog_image"; do
	if regctl image digest "$image" >/dev/null 2>&1; then
		fail "$image already exists; refusing to overwrite a published OLM artifact"
	fi
done

pin() {
	local image=$1 digest
	digest=$(regctl image digest "$image")
	[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || fail "invalid digest for $image: $digest"
	printf '%s@%s\n' "$image" "$digest"
}
chart_image="${HELM_CHART#oci://}:$HELM_CHART_VERSION"
chart_ref=$(pin "$chart_image")

locked_image() {
	local key=$1 value
	value=$(awk -F= -v key="$key" '$1 == key {print $2}' "$digests")
	# Missing, duplicate and unpinned entries all fail; never source this file.
	[[ $value =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || fail "invalid or missing $key in $digests"
	printf '%s\n' "$value"
}
operator=$(locked_image OPERATOR)
validator=$(locked_image VALIDATOR)
while read -r key env_name; do
	export RELATED_NAME="$env_name"
	RELATED_REF=$(locked_image "$key")
	export RELATED_REF
	yq -e 'select(.kind == "Deployment").spec.template.spec.containers[] |
		select(.name == "manager").env[] | select(.name == strenv(RELATED_NAME))' \
		config/manager/manager.yaml >/dev/null
	yq -i '(select(.kind == "Deployment").spec.template.spec.containers[] |
		select(.name == "manager").env[] | select(.name == strenv(RELATED_NAME))).value = strenv(RELATED_REF)' \
		config/manager/manager.yaml
done <<'IMAGES'
OPERATOR RELATED_IMAGE_RBLN_NPU_OPERATOR
VALIDATOR RELATED_IMAGE_RBLN_NPU_OPERATOR_VALIDATOR
DEVICE_PLUGIN RELATED_IMAGE_RBLN_K8S_DEVICE_PLUGIN
METRICS_EXPORTER RELATED_IMAGE_RBLN_METRICS_EXPORTER
NPU_FEATURE_DISCOVERY RELATED_IMAGE_RBLN_NPU_FEATURE_DISCOVERY
VFIO_MANAGER RELATED_IMAGE_VFIO_MANAGER
CONTAINER_TOOLKIT RELATED_IMAGE_RBLN_CONTAINER_TOOLKIT
DRA_KUBELET_PLUGIN RELATED_IMAGE_DRA_KUBELET_PLUGIN
SANDBOX_DEVICE_PLUGIN RELATED_IMAGE_SANDBOX_DEVICE_PLUGIN
DRIVER_MANAGER RELATED_IMAGE_DRIVER_MANAGER
IMAGES

make bundle VERSION="$VERSION" IMAGE="$operator" USE_IMAGE_DIGESTS=false
export OCP_RANGE=${OCP_RANGE:-$(awk '/^OCP_RANGE / {print $3}' versions.mk)}
export OPERATOR_REF="$operator"
yq -i '.annotations."com.redhat.openshift.versions" = strenv(OCP_RANGE)' bundle/metadata/annotations.yaml
csv=bundle/manifests/rbln-npu-operator.clusterserviceversion.yaml
yq -i '.metadata.annotations.containerImage = strenv(OPERATOR_REF)' "$csv"
make operator-sdk
# make may select a system operator-sdk instead of bin/operator-sdk.
sdk=./bin/operator-sdk
[ -x "$sdk" ] || sdk="operator-sdk"
"$sdk" bundle validate ./bundle
if [ "$kind" = rc ]; then
	"$sdk" bundle validate ./bundle --select-optional suite=operatorframework
	"$sdk" bundle validate ./bundle --select-optional name=operatorhub
fi
csv_name=$(yq -er '.metadata.name' "$csv")
csv_version=$(yq -er '.spec.version' "$csv")
[ "$csv_name" = "rbln-npu-operator.v${VERSION#v}" ] && [ "$csv_version" = "${VERSION#v}" ] ||
	fail "generated CSV does not match $VERSION: $csv_name ($csv_version)"

make build-bundle-image BUNDLE_IMAGE="$bundle_image" PLATFORM=linux/amd64 PUSH_ON_BUILD=true
bundle_ref=$(pin "$bundle_image")
make catalog VERSION="$VERSION" CATALOG_BUNDLE="$bundle_ref"
make catalog-build-image CATALOG_IMAGE="$catalog_image" PLATFORM=linux/amd64 PUSH_ON_BUILD=true
catalog_ref=$(pin "$catalog_image")

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
