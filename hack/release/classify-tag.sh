#!/usr/bin/env bash
# Classify a pushed version tag for release.yaml and enforce the invariants
# that make "what was validated is what ships" true.
#
#   hack/release/classify-tag.sh <tag>      # prints key=value lines for $GITHUB_OUTPUT
#
#   vX.Y.Z-rcN  -> kind=rc   the tag must sit on release-X.Y.Z
#   vX.Y.Z      -> kind=ga   additionally, the tag must be the exact commit of
#                            the newest rc for X.Y.Z, and that commit must carry
#                            a passing `rc-validation` commit status (posted by
#                            the test-infra matrix build). Anything else fails
#                            here, before a single artifact is published.
#
# The status check is enforced when RC_VALIDATION_ENFORCE=true and only warns
# otherwise, so this gate can ship before the validation pipeline reports.
# Only key=value lines go to stdout (the workflow tees stdout into
# $GITHUB_OUTPUT); everything else goes to stderr.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

tag=${1:?usage: classify-tag.sh <tag>}
[[ $tag =~ ^v([0-9]+\.[0-9]+\.[0-9]+)(-rc([0-9]+))?$ ]] ||
	fail "tag must be vX.Y.Z or vX.Y.Z-rcN (got '$tag')"
version=${BASH_REMATCH[1]}
rcn=${BASH_REMATCH[3]:-}
branch="release-$version"

git fetch --quiet origin "refs/heads/$branch:refs/remotes/origin/$branch" '+refs/tags/*:refs/tags/*' ||
	fail "branch $branch does not exist on origin; version tags live on release branches only"
sha=$(git rev-parse "$tag^{commit}")
git merge-base --is-ancestor "$sha" "origin/$branch" ||
	fail "$tag (${sha:0:12}) is not a commit of $branch"

if [ -n "$rcn" ]; then
	kind=rc
	last_rc=$tag
else
	kind=ga
	last_rc=$(last_rc_tag "$version")
	[ -n "$last_rc" ] || fail "no rc tag for $version; GA promotes a validated rc, it does not build"
	rc_sha=$(git rev-parse "$last_rc^{commit}")
	[ "$rc_sha" = "$sha" ] ||
		fail "GA tag $tag (${sha:0:12}) is not the commit of $last_rc (${rc_sha:0:12}). Tag a new rc, validate it, then GA. Nothing was published."

	state=$(commit_status_state "$sha" rc-validation)
	if [ "$state" = success ]; then
		echo "ok    rc-validation on $last_rc (${sha:0:12}) is success" >&2
	else
		msg="rc-validation status on $last_rc (${sha:0:12}) is '$state', not 'success'"
		if [ "${RC_VALIDATION_ENFORCE:-}" = true ]; then
			fail "$msg. 'error' means the validation infrastructure failed (rebuild it); 'failure' means the rc is broken (fix on main, cut a new rc); 'none' means it never ran. Nothing was published."
		fi
		echo "::warning::$msg; continuing because RC_VALIDATION_ENFORCE is not 'true'" >&2
	fi
fi

printf 'kind=%s\nversion=%s\nbranch=%s\nlast_rc=%s\nsha=%s\n' \
	"$kind" "$version" "$branch" "$last_rc" "$sha"
