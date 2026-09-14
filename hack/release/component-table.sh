#!/usr/bin/env bash
# Component-versions table for the release notes. The release kit runs this
# through its release_notes_extra input and appends stdout after Known Issues.
#
#   hack/release/component-table.sh      # markdown on stdout
#
# Lists the images the Helm chart pins at the checked-out commit. The OLM
# bundle pins the same images by digest (rc-digests in release.yaml).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
command -v yq >/dev/null || { echo "ERROR: yq is required" >&2; exit 1; }

values=deployments/rbln-npu-operator/values.yaml

echo "## Component versions"
echo
echo "Images pinned by the Helm chart at this tag (\`$values\`). The OLM bundle pins the same images by digest."
echo
echo "| Component | Image |"
echo "|---|---|"
for entry in \
	"Device plugin|.devicePlugin.image" \
	"Metrics exporter|.metricsExporter.image" \
	"NPU feature discovery|.npuFeatureDiscovery.image" \
	"VFIO manager|.vfioManager.image" \
	"Container toolkit|.containerToolkit.image" \
	"DRA kubelet plugin|.draKubeletPlugin.image" \
	"Sandbox device plugin|.sandboxDevicePlugin.image" \
	"Driver manager|.driver.manager.image"; do
	name=${entry%%|*}
	path=${entry#*|}
	echo "| $name | \`$(yq "${path}.registry" "$values")/$(yq "${path}.repository" "$values"):$(yq "${path}.tag" "$values")\` |"
done
nr=".driver.upgradePolicy.reboot.image"
echo "| Node reboot | \`$(yq "${nr}.registry" "$values")/$(yq "${nr}.image" "$values"):$(yq "${nr}.version" "$values")\` |"
echo
echo "Driver (\`rbln-driver\`) and \`rbln-smd\` versions are chosen per \`RBLNDriver\` CR and are not pinned by this release."
