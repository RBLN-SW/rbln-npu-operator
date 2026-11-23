#!/usr/bin/env bash
# Component-versions table for the release notes. The release kit runs this
# through its release_notes_extra input and appends stdout after Known Issues.
#
#   hack/release/component-table.sh      # markdown on stdout
#
# Contract releases read the frozen input; older callers use checkout defaults.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
command -v yq >/dev/null || { echo "ERROR: yq is required" >&2; exit 1; }

values=deployments/rbln-npu-operator/values.yaml

echo "## Component versions"
echo
lock=${RELEASE_INPUT:-dist/release-input.json}
if [ -n "${RELEASE_INPUT:-}" ] || [ -f "$lock" ]; then
	echo "Images in the published Helm chart and OLM bundle."
	echo
	echo "| Component | Image |"
	echo "|---|---|"
	jq -er '.components | to_entries[] | "| \(.key) | `\(.value.registry)/\(.value.repository):\(.value.tag)@\(.value.digest)` |"' "$lock"
else
	echo "Images pinned by the Helm chart at this tag (\`$values\`). The OLM bundle pins the same images by digest."
	echo
	echo "| Component | Image |"
	echo "|---|---|"
	for entry in \
		"Device plugin|.devicePlugin.image" \
		"Metrics exporter|.metricsExporter.image" \
		"NPU feature discovery|.npuFeatureDiscovery.image" \
		"Container toolkit|.containerToolkit.image" \
		"DRA kubelet plugin|.draKubeletPlugin.image" \
		"Sandbox device plugin|.sandboxDevicePlugin.image" \
		"Driver manager|.driver.manager.image"; do
		name=${entry%%|*}
		path=${entry#*|}
		echo "| $name | \`$(yq "${path}.registry" "$values")/$(yq "${path}.repository" "$values"):$(yq "${path}.tag" "$values")\` |"
	done
	echo
fi
echo
echo "### Default driver versions"
echo
echo "Driver deployment is disabled by default. Enabling \`driver.enabled\` uses these family instances; each \`rbln-smd\` tag follows its driver's version. Override \`driver.instances.<family>.image.tag\` to change it."
echo
echo "| NPU family | Driver version | rbln-smd version |"
echo "|---|---|---|"
while IFS= read -r family; do
	version=$(yq ".driver.instances[\"$family\"].image.tag // .driver.image.tag" "$values")
	echo "| $family | \`$version\` | \`$version\` |"
done < <(yq '.driver.instances | keys | .[]' "$values")
echo
echo "The optional fallback CR uses \`driver.image.tag\` (\`$(yq '.driver.image.tag' "$values")\`) when \`driver.deployDefault=true\`. These are Helm defaults; other \`RBLNDriver\` CRs choose their own versions."
