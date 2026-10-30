#!/usr/bin/env bash
# Copyright 2026 Rebellions Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Emit the image lock shared by dev/rc OLM publishing and GA bundle generation.
# resolve-component-digests.sh <operator image> <validator image> [values.yaml]
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

resolve OPERATOR "$operator"
resolve VALIDATOR "$validator"
while read -r key path; do
	registry=$(yq -er "${path}.registry" "$values")
	repository=$(yq -er "${path}.repository" "$values")
	tag=$(yq -er "${path}.tag" "$values")
	resolve "$key" "$registry/$repository:$tag"
done <<'COMPONENTS'
DEVICE_PLUGIN .devicePlugin.image
METRICS_EXPORTER .metricsExporter.image
NPU_FEATURE_DISCOVERY .npuFeatureDiscovery.image
VFIO_MANAGER .vfioManager.image
CONTAINER_TOOLKIT .containerToolkit.image
DRA_KUBELET_PLUGIN .draKubeletPlugin.image
SANDBOX_DEVICE_PLUGIN .sandboxDevicePlugin.image
DRIVER_MANAGER .driver.manager.image
COMPONENTS
