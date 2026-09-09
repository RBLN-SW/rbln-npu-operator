#!/usr/bin/env bash
# Cut a release branch and tag its first release candidate.
#
#   hack/release/cut-release.sh <minor|patch|major> [--version X.Y.Z]
#
# The kind decides both the number and where the branch starts:
#   minor   release-X.(Y+1).0  from origin/main        everything merged since the last GA
#   patch   release-X.Y.(Z+1)  from the newest GA tag  fixes only, arriving as backports
#   major   release-(X+1).0.0  from origin/main        an explicit decision (e.g. 1.0)
# --version overrides the computed number; the kind still decides the start.
#
# Creates release-X.Y.Z, tags the same commit vX.Y.Z-rc1, pushes both, and
# creates the "backport release-X.Y.Z" label the backport workflow reacts to.
# The rc tag push is what starts the rc pipeline in .github/workflows/release.yaml;
# this script itself builds nothing.
#
# A patch is refused unless its parent is the newest GA: a lower version
# published after a higher one would enter the OLM channels below the head.
# Requires push rights on origin (the release GitHub App in CI, or a
# bypass-listed release manager locally) and an authenticated gh.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

usage="usage: cut-release.sh <minor|patch|major> [--version X.Y.Z]"
kind=${1:?$usage}
shift
version=""
while [ $# -gt 0 ]; do
	case $1 in
	--version) version=${2:?$usage}; shift 2 ;;
	*) fail "unknown argument: $1 ($usage)" ;;
	esac
done
[[ $kind =~ ^(minor|patch|major)$ ]] || fail "kind must be minor, patch or major (got '$kind')"

git fetch --quiet origin main '+refs/tags/*:refs/tags/*'
last=$(last_ga_tag)

case $kind in
patch)
	[ -n "$last" ] || fail "no GA tag on origin; there is nothing to patch"
	from=$last
	;;
minor | major)
	from=origin/main
	;;
esac
sha=$(git rev-parse --verify "$from^{commit}")

if [ -z "$version" ]; then
	[ -n "$last" ] || fail "no GA tag on origin; pass --version for the first release"
	version=$(next_version "$kind" "$last")
fi
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version must be X.Y.Z (got '$version')"
IFS=. read -r x y z <<<"$version"

if [ "$kind" = patch ]; then
	[ "$z" -gt 0 ] || fail "a patch needs Z > 0 (got $version)"
	parent="v$x.$y.$((z - 1))"
	[ "$parent" = "$last" ] ||
		fail "$version would patch $parent, but the newest GA is $last. Only the newest GA can be patched; the OLM graph must stay linear."
fi
if [ -n "$last" ] && [ "$(printf '%s\n%s\n' "${last#v}" "$version" | sort -V | tail -1)" != "$version" ]; then
	fail "version $version does not sort above the newest GA $last"
fi
[ "v$version" != "$last" ] || fail "version $version is already GA"

branch="release-$version"
tag="v$version-rc1"
if git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
	fail "branch $branch already exists on origin"
fi
if [ -n "$(git ls-remote --tags origin "refs/tags/v$version" "refs/tags/v$version-rc*")" ]; then
	fail "tags for v$version already exist on origin"
fi

case $kind in
patch)
	note "$branch starts at $last; fixes arrive as backports from main"
	;;
*)
	note "commits on main since ${last:-<no GA tag>} (informational)"
	if [ -n "$last" ]; then
		summarize_commits "$last..$sha" "$kind"
	else
		summarize_commits "$sha" "$kind"
	fi
	;;
esac

note "creating $branch and $tag at ${sha:0:12} ($kind from $from)"
git branch --no-track "$branch" "$sha"
git tag -a "$tag" "$sha" -m "$version release candidate 1"
git push origin "refs/heads/$branch" "refs/tags/$tag"

note "creating label 'backport $branch'"
gh label create "backport $branch" --color 0E8A16 \
	--description "Cherry-pick this merged fix into $branch (next rc)" --force

cat <<SUMMARY

cut complete
  kind   : $kind (from $from)
  branch : $branch @ ${sha:0:12}
  tag    : $tag  (release.yaml rc path is now running)
  label  : backport $branch
SUMMARY

# Let the calling workflow name the result (Slack, summary) without parsing.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
	printf 'version=%s\nbranch=%s\ntag=%s\n' "$version" "$branch" "$tag" >>"$GITHUB_OUTPUT"
fi
