#!/usr/bin/env bash
# Render a bundle in the caller's build checkout from the frozen release lock.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
. hack/release/lib.sh
digests=${1:?component lock required}
VERSION=${2:?version required}
kind=${3:?phase required}

locked_image() {
	local key=$1 value
	value=$(awk -F= -v key="$key" '$1 == key {print $2}' "$digests")
	# Missing, duplicate and unpinned entries all fail; never source this file.
	[[ $value =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || fail "invalid or missing $key in $digests"
	printf '%s\n' "$value"
}
operator=$(locked_image OPERATOR)
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
CONTAINER_TOOLKIT RELATED_IMAGE_RBLN_CONTAINER_TOOLKIT
DRA_KUBELET_PLUGIN RELATED_IMAGE_DRA_KUBELET_PLUGIN
SANDBOX_DEVICE_PLUGIN RELATED_IMAGE_SANDBOX_DEVICE_PLUGIN
DRIVER_MANAGER RELATED_IMAGE_DRIVER_MANAGER
IMAGES

# Device Config Manager currently ships in dev only. Add its related image
# when present in the lock without requiring it in existing release locks.
if grep -q '^DEVICE_CONFIG_MANAGER=' "$digests"; then
	DEVICE_CONFIG_MANAGER_REF=$(locked_image DEVICE_CONFIG_MANAGER)
	export DEVICE_CONFIG_MANAGER_REF
	yq -i '(select(.kind == "Deployment").spec.template.spec.containers[] |
		select(.name == "manager").env) |=
		(map(select(.name != "RELATED_IMAGE_RBLN_K8S_DEVICE_CONFIG_MANAGER")) +
		[{"name": "RELATED_IMAGE_RBLN_K8S_DEVICE_CONFIG_MANAGER", "value": strenv(DEVICE_CONFIG_MANAGER_REF)}])' \
		config/manager/manager.yaml
fi

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
