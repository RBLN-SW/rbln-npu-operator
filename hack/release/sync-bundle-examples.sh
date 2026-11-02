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

# Align OperatorHub CR examples with the images declared by the generated CSV.
# Both dev/rc publishing and GA submission call this through `make bundle`.
# Driver/SMD versions and node selectors remain per-CR sample configuration;
# their family/OS/kernel-specific images are not in the global relatedImages.
# Usage: sync-bundle-examples.sh <CSV file> [--check]
set -euo pipefail

csv=${1:?usage: sync-bundle-examples.sh <CSV file> [--check]}
mode=${2:-write}
if [ "$#" -gt 2 ] || { [ "$mode" != write ] && [ "$mode" != --check ]; }; then
	echo "usage: sync-bundle-examples.sh <CSV file> [--check]" >&2
	exit 1
fi
for tool in yq jq; do
	command -v "$tool" >/dev/null || { echo "$tool is required to synchronize OLM examples" >&2; exit 1; }
done

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
yq -o=json '.' "$csv" | jq -e '
  # Keep tag-based CR image references, as in the Helm chart. Digests remain
  # in relatedImages; dropping the tag would break tag-based mirror mappings.
  # Splitting at the first slash also preserves registry ports and nested repos.
  def image_for($images; $name):
    [$images[] | select(.name == $name)] as $matches |
    if ($matches | length) != 1 then
      error("expected exactly one relatedImages entry for " + $name)
    else
      ($matches[0].image | split("@")[0]) as $ref |
      if ($ref | test("^[^/]+/.+:[^/:@]+$")) then
        $ref | capture("^(?<registry>[^/]+)/(?<image>.+):(?<version>[^/:@]+)$")
      else error("a registry/repository:tag reference is required for " + $name) end
    end;

  [
    {kind: "RBLNClusterPolicy", path: ["spec", "validator"], name: "rbln-npu-operator-validator"},
    {kind: "RBLNClusterPolicy", path: ["spec", "devicePlugin"], name: "rbln-k8s-device-plugin"},
    {kind: "RBLNClusterPolicy", path: ["spec", "metricsExporter"], name: "rbln-metrics-exporter"},
    {kind: "RBLNClusterPolicy", path: ["spec", "npuFeatureDiscovery"], name: "rbln-npu-feature-discovery"},
    {kind: "RBLNClusterPolicy", path: ["spec", "containerToolkit"], name: "rbln-container-toolkit"},
    {kind: "RBLNClusterPolicy", path: ["spec", "draKubeletPlugin"], name: "dra-kubelet-plugin"},
    {kind: "RBLNClusterPolicy", path: ["spec", "sandboxDevicePlugin"], name: "sandbox-device-plugin"},
    {kind: "RBLNClusterPolicy", path: ["spec", "vfioManager"], name: "vfio-manager"},
    {kind: "RBLNClusterPolicy", path: ["spec", "vfioManager", "driverManager"], name: "driver-manager"},
    {kind: "RBLNDriver", path: ["spec", "manager"], name: "driver-manager"}
  ] as $targets |
  .spec.relatedImages as $images |
  (.metadata.annotations."alm-examples" | fromjson) as $examples |
  if ($examples | type) != "array" then error("alm-examples must be an array")
  elif ($examples | any(.kind == "RBLNClusterPolicy") | not) then
    error("alm-examples must include an RBLNClusterPolicy")
  else
    {original: $examples, expected: [
      $examples[] | .kind as $kind |
      reduce ($targets[] | select(.kind == $kind)) as $target (.;
        if (getpath($target.path) | type) != "object" then
          error("missing example image configuration: " + ($target.path | join(".")))
        else
          setpath($target.path; getpath($target.path) + image_for($images; $target.name))
        end)
    ]}
  end
' > "$tmp"

if jq -e '.original == .expected' "$tmp" >/dev/null; then
	exit 0
fi
if [ "$mode" = --check ]; then
	echo "OLM example images do not match relatedImages in $csv" >&2
	exit 1
fi
BUNDLE_EXAMPLES_JSON=$(jq '.expected' "$tmp")
export BUNDLE_EXAMPLES_JSON
yq -i '.metadata.annotations."alm-examples" = strenv(BUNDLE_EXAMPLES_JSON)' "$csv"
echo "Synchronized OLM example images in $csv"
