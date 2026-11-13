#!/usr/bin/env bash
# Emit the image lock shared by dev/rc OLM publishing and GA bundle generation.
# resolve-component-digests.sh <operator image> <validator image> [values.yaml]
# Set INCLUDE_DEVICE_CONFIG_MANAGER=true for dev bundles; its Docker Hub
# release image is not published yet, so release locks do not include it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

operator=${1:?operator image is required}
validator=${2:?validator image is required}
values=${3:-deployments/rbln-npu-operator/values.yaml}

resolve() {
	local key=$1 image=$2 digest
	digest=$(regctl image digest "$image")
	[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "Invalid digest for $image: $digest" >&2; exit 1; }
	printf '%s=%s@%s\n' "$key" "$image" "$digest"
}

resolve_component() {
	local key=$1 path=$2 registry repository tag
	registry=$(yq -er "${path}.registry" "$values")
	repository=$(yq -er "${path}.repository" "$values")
	tag=$(yq -er "${path}.tag" "$values")
	resolve "$key" "$registry/$repository:$tag"
}

resolve OPERATOR "$operator"
resolve VALIDATOR "$validator"
while read -r key path; do
	resolve_component "$key" "$path"
done <<'COMPONENTS'
DEVICE_PLUGIN .devicePlugin.image
METRICS_EXPORTER .metricsExporter.image
NPU_FEATURE_DISCOVERY .npuFeatureDiscovery.image
CONTAINER_TOOLKIT .containerToolkit.image
DRA_KUBELET_PLUGIN .draKubeletPlugin.image
SANDBOX_DEVICE_PLUGIN .sandboxDevicePlugin.image
DRIVER_MANAGER .driver.manager.image
COMPONENTS
if [ "${INCLUDE_DEVICE_CONFIG_MANAGER:-false}" = true ]; then
	resolve_component DEVICE_CONFIG_MANAGER .deviceConfigManager.image
fi
