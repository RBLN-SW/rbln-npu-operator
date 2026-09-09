#!/usr/bin/env bash
# Generate the release notes draft for a GA (or rc) tag.
#
#   hack/release/release-notes.sh <tag> [<previous GA tag>] [<output file>]
#
# The range is <previous GA>..<tag>. Because release branches carry
# cherry-picks of main commits, the raw log would list a fix twice; two kinds
# of commits are dropped:
#   - commits in the range that carry a "(cherry picked from commit <sha>)"
#     trailer: their origin on main is in the range and is listed instead
#   - main commits whose sha appears as such a trailer in the previous GA's
#     own backports: that fix already shipped in the previous release
# Commits are grouped by Conventional Commit type, as before, and a table of
# the component images pinned by the chart is appended.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

tag=${1:?usage: release-notes.sh <tag> [<prev GA tag>] [<out>]}
prev=${2:-}
out=${3:-release_notes.md}
repo=${GITHUB_REPOSITORY:-rebellions-sw/rbln-npu-operator}
values=deployments/rbln-npu-operator/values.yaml

trailer_shas() { # <range> -> origin shas named in cherry-pick trailers
	git log --format=%B "$1" | sed -n 's/^(cherry picked from commit \([0-9a-f]\{40\}\))$/\1/p' | sort -u
}

if [ -n "$prev" ]; then
	range="$prev..$tag"
	base=$(git merge-base "$prev" "$tag")
	shipped=$(trailer_shas "$base..$prev")
else
	range=$tag
	shipped=""
fi

commits=$(mktemp)
trap 'rm -f "$commits"' EXIT
for c in $(git rev-list --no-merges "$range"); do
	body=$(git log -1 --format=%B "$c")
	grep -q '^(cherry picked from commit ' <<<"$body" && continue # a backport; its origin is listed
	grep -qx -- "$c" <<<"$shipped" && continue                    # already shipped via the previous GA
	git log -1 --format="%s ([%h](https://github.com/$repo/commit/%H))" "$c" |
		sed -E "s|\(#([0-9]+)\)|([#\1](https://github.com/$repo/pull/\1))|g" >>"$commits"
done

get() { grep -E "^$1(\(.*\))?!?:" "$commits" || true; }
bullets() { sed 's/^/- /'; }
breaking=$(grep -E '^[a-z]+(\(.*\))?!:' "$commits" || true)
feats=$(get feat)
fixes=$(get fix)
improvements=$(for t in docs refactor perf style build chore test ci; do get "$t"; done)

{
	echo "# Release Notes"
	echo
	if [ -n "$breaking" ]; then
		echo "## ⚠ Breaking Changes"
		bullets <<<"$breaking"
		echo
		echo "> Review this version's migration notes under [docs/](https://github.com/$repo/tree/$tag/docs) before upgrading."
		echo
	fi
	echo "## New Features"
	if [ -n "$feats" ]; then bullets <<<"$feats"; else echo "- None"; fi
	echo
	echo "## Improvements"
	if [ -n "$improvements" ]; then bullets <<<"$improvements"; else echo "- None"; fi
	echo
	echo "## Fixed Issues"
	if [ -n "$fixes" ]; then bullets <<<"$fixes"; else echo "- None"; fi
	echo
	echo "## Known Issues"
	echo "- TBD"
	echo
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
		img="$(yq "${path}.registry" "$values")/$(yq "${path}.repository" "$values"):$(yq "${path}.tag" "$values")"
		echo "| $name | \`$img\` |"
	done
	nr=".driver.upgradePolicy.reboot.image"
	echo "| Node reboot | \`$(yq "${nr}.registry" "$values")/$(yq "${nr}.image" "$values"):$(yq "${nr}.version" "$values")\` |"
	echo
	echo "Driver (\`rbln-driver\`) and \`rbln-smd\` versions are chosen per \`RBLNDriver\` CR and are not pinned by this release."
	echo
	if [ -n "$prev" ]; then
		echo "_Changes since $prev. Generated for $tag._"
	else
		echo "_Generated for $tag._"
	fi
} >"$out"

echo "wrote $out ($(grep -c '^- ' "$out") bullet(s), range ${prev:-<start>}..$tag)"
