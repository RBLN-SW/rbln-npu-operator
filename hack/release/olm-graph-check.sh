#!/usr/bin/env bash
# Structural check of the certified-operators upgrade graph for a new version.
#
#   hack/release/olm-graph-check.sh <X.Y.Z> [<certified-operators checkout>]
#
# Run at rc time (and again right before the GA bundle PR) so that the Red Hat
# pipeline sees a valid graph on the first try. Without a checkout path the
# upstream repo is cloned sparsely (public, no credentials needed).
#
# For every OCP minor in OCP_RANGE (default: the bundle annotation) and every
# channel the release workflow appends to (stable, fast, candidate):
#   - the version directory must not exist yet (merged versions are immutable)
#   - the new entry must not already be in the channel
#   - the new version must sort above the current channel head, otherwise the
#     pipeline would emit a downgrade edge or a second head
#   - after appending "new replaces head", the channel has exactly one head
#
# This is deliberately structural (yq only). It does not render bundles with
# opm because rc bundle images are not pullable by anyone but us; the render
# itself is exercised by `make catalog` in PR CI.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

version=${1:?usage: olm-graph-check.sh <X.Y.Z> [<checkout>]}
checkout=${2:-}
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version must be X.Y.Z (got '$version')"
command -v yq >/dev/null || fail "yq is required"

pkg=rbln-npu-operator
new="$pkg.v$version"
upstream=${CERTIFIED_OPERATORS_UPSTREAM:-https://github.com/redhat-openshift-ecosystem/certified-operators.git}
range=${OCP_RANGE:-$(yq '.annotations."com.redhat.openshift.versions" // ""' bundle/metadata/annotations.yaml 2>/dev/null || true)}
[[ $range =~ ^v4\.([0-9]+)-v4\.([0-9]+)$ ]] ||
	fail "OCP_RANGE must look like v4.16-v4.22 (got '${range:-<empty>}'). release.yaml sets it; locally run with OCP_RANGE=v4.16-v4.22."
lo=${BASH_REMATCH[1]}
hi=${BASH_REMATCH[2]}

if [ -z "$checkout" ]; then
	checkout=$(mktemp -d)
	trap 'rm -rf "$checkout"' EXIT
	note "sparse-cloning $upstream"
	git clone --quiet --depth 1 --filter=blob:none --sparse "$upstream" "$checkout"
	git -C "$checkout" sparse-checkout set --no-cone "operators/$pkg/ci.yaml" "operators/$pkg/catalog-templates" "operators/$pkg/*/" >/dev/null 2>&1 ||
		git -C "$checkout" sparse-checkout set "operators/$pkg"
fi
opdir="$checkout/operators/$pkg"
[ -d "$opdir" ] || fail "no operators/$pkg in $checkout"

problems=0
problem() {
	echo "FAIL  $*"
	problems=$((problems + 1))
}
pass() { echo "ok    $*"; }

if [ -d "$opdir/$version" ]; then
	problem "operators/$pkg/$version already exists upstream (merged versions are immutable; bump the version)"
else
	pass "version directory $version is new"
fi

for m in $(seq "$lo" "$hi"); do
	ocp="v4.$m"
	tpl="$opdir/catalog-templates/$ocp.yaml"
	if [ ! -f "$tpl" ]; then
		echo "info  $ocp: no template yet; the release workflow will create a skeleton with $new as the only entry"
		continue
	fi
	for ch in stable fast candidate; do
		sel=".entries[] | select(.schema == \"olm.channel\" and .name == \"$ch\")"
		names=$(yq "$sel | .entries[].name" "$tpl" 2>/dev/null || true)
		replaced=$(yq "$sel | .entries[].replaces // \"\"" "$tpl" 2>/dev/null | grep -v '^$' || true)

		if grep -qx -- "$new" <<<"$names"; then
			problem "$ocp/$ch: $new is already an entry"
			continue
		fi

		head=$(grep "^$pkg\.v" <<<"$names" | sort -V | tail -1 || true)
		if [ -n "$head" ]; then
			hv=${head#"$pkg".v}
			top=$(printf '%s\n%s\n' "$hv" "$version" | sort -V | tail -1)
			if [ "$top" != "$version" ]; then
				problem "$ocp/$ch: $version does not sort above current head $hv; the pipeline would emit '$new replaces $head' as a downgrade or leave two heads"
				continue
			fi
		fi

		# Simulate appending "new replaces head". A head is an entry no other
		# entry replaces; a dangling target is a replaces value with no entry.
		ents=$(printf '%s\n' "$names" "$new" | grep -v '^$' | sort -u)
		reps=$(printf '%s\n' "$replaced" "$head" | grep -v '^$' | sort -u)
		real_heads=$(comm -23 <(printf '%s\n' "$ents") <(printf '%s\n' "$reps"))
		dangling=$(comm -13 <(printf '%s\n' "$ents") <(printf '%s\n' "$reps"))
		if [ -n "$dangling" ]; then
			problem "$ocp/$ch: replaces targets missing from the channel: $(tr '\n' ' ' <<<"$dangling")"
		fi
		count=$(grep -c . <<<"$real_heads" || true)
		if [ "$count" -eq 1 ]; then
			pass "$ocp/$ch: '$new replaces ${head:-<none>}' yields a single head"
		else
			problem "$ocp/$ch: $count channel heads after adding $new: $(tr '\n' ' ' <<<"$real_heads")"
		fi
	done
done

echo
if [ "$problems" -gt 0 ]; then
	echo "olm-graph-check: $problems problem(s) for $new over $range"
	exit 1
fi
echo "olm-graph-check: $new is a valid next head for every channel in $range"
