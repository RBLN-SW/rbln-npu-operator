#!/usr/bin/env bash
# Tag the HEAD of a release branch as the next release candidate or as GA.
#
#   hack/release/tag-release.sh <release-X.Y.Z> rc     # vX.Y.Z-rc<N+1>
#   hack/release/tag-release.sh <release-X.Y.Z> ga     # vX.Y.Z
#
# rc: refuses when HEAD already carries an rc tag (nothing new to validate) or
#     when the version has already gone GA.
# ga: refuses unless HEAD is exactly the commit of the newest rc, and unless
#     that commit carries a passing `rc-validation` commit status (enforced
#     when RC_VALIDATION_ENFORCE=true, warn-only otherwise). release.yaml
#     re-checks both before anything is published, so a mistaken tag never
#     reaches a registry.
#
# The tag push is the trigger for release.yaml; this script publishes nothing.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

branch=${1:?usage: tag-release.sh <release-X.Y.Z> <rc|ga>}
kind=${2:?usage: tag-release.sh <release-X.Y.Z> <rc|ga>}

[[ $branch =~ ^release-([0-9]+\.[0-9]+\.[0-9]+)$ ]] || fail "branch must be release-X.Y.Z (got '$branch')"
version=${BASH_REMATCH[1]}
ga_tag="v$version"

git fetch --quiet origin "refs/heads/$branch:refs/remotes/origin/$branch" '+refs/tags/*:refs/tags/*'
head=$(git rev-parse "origin/$branch")

if git rev-parse -q --verify "refs/tags/$ga_tag" >/dev/null; then
	fail "$ga_tag already exists: $branch is released and read-only. A fix ships as a patch: cut-release.sh patch."
fi

last_rc=$(last_rc_tag "$version")

case $kind in
rc)
	if [ -n "$last_rc" ] && [ "$(git rev-parse "$last_rc^{commit}")" = "$head" ]; then
		fail "HEAD of $branch is already tagged $last_rc; nothing new to release"
	fi
	if [ -n "$last_rc" ]; then
		n=${last_rc##*-rc}
		tag="v$version-rc$((n + 1))"
	else
		tag="v$version-rc1"
	fi
	msg="$version release candidate ${tag##*-rc}"
	;;
ga)
	[ -n "$last_rc" ] || fail "no rc tag for $version; GA must promote a validated rc"
	rc_sha=$(git rev-parse "$last_rc^{commit}")
	[ "$rc_sha" = "$head" ] ||
		fail "HEAD of $branch (${head:0:12}) is not the commit of $last_rc (${rc_sha:0:12}); tag a new rc and validate it first"
	state=$(commit_status_state "$head" rc-validation)
	if [ "$state" = success ]; then
		note "rc-validation on $last_rc is success"
	else
		status_msg="rc-validation status on $last_rc (${head:0:12}) is '$state', not 'success'"
		if [ "${RC_VALIDATION_ENFORCE:-}" = true ]; then
			fail "$status_msg. 'error' = validation infrastructure failed, rebuild it; 'failure' = the rc is broken, fix on main and cut a new rc; 'none' = it never ran."
		fi
		echo "WARN: $status_msg; continuing because RC_VALIDATION_ENFORCE is not 'true'" >&2
	fi
	tag=$ga_tag
	msg="$version"
	;;
*)
	fail "kind must be rc or ga (got '$kind')"
	;;
esac

note "tagging $tag at ${head:0:12} on $branch"
git tag -a "$tag" "$head" -m "$msg"
git push origin "refs/tags/$tag"
echo "tagged $tag (release.yaml ${kind} path is now running)"
