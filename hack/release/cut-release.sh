#!/usr/bin/env bash
# Cut a release branch from main and tag its first release candidate.
#
#   hack/release/cut-release.sh <X.Y.Z> [<sha|ref>]
#
# Creates release-X.Y.Z at <ref> (default origin/main), tags the same commit
# vX.Y.Z-rc1, pushes both, and creates the "backport release-X.Y.Z" label the
# backport workflow reacts to. The rc tag push is what starts the rc pipeline
# in .github/workflows/release.yaml; this script itself builds nothing.
#
# The version is the release manager's call. The Conventional Commit tally
# printed below is informational only. Requires push rights on origin (the
# release GitHub App in CI, or a bypass-listed release manager locally) and an
# authenticated gh.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# shellcheck source=hack/release/lib.sh
. hack/release/lib.sh

version=${1:?usage: cut-release.sh <X.Y.Z> [<sha|ref>]}
from=${2:-origin/main}

[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version must be X.Y.Z (got '$version')"
branch="release-$version"
tag="v$version-rc1"

git fetch --quiet origin main '+refs/tags/*:refs/tags/*'
sha=$(git rev-parse --verify "$from^{commit}" 2>/dev/null) || fail "unknown ref: $from"
git merge-base --is-ancestor "$sha" origin/main || fail "$from (${sha:0:12}) is not on main"
if git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
	fail "branch $branch already exists on origin"
fi
if [ -n "$(git ls-remote --tags origin "refs/tags/v$version" "refs/tags/v$version-rc*")" ]; then
	fail "tags for v$version already exist on origin"
fi

last=$(last_ga_tag)
note "commits on main since ${last:-<no GA tag>} (informational)"
if [ -n "$last" ]; then
	summarize_commits "$last..$sha" "$last"
else
	summarize_commits "$sha"
fi

note "creating $branch and $tag at ${sha:0:12}"
git branch --no-track "$branch" "$sha"
git tag -a "$tag" "$sha" -m "$version release candidate 1"
git push origin "refs/heads/$branch" "refs/tags/$tag"

note "creating label 'backport $branch'"
gh label create "backport $branch" --color 0E8A16 \
	--description "Cherry-pick this merged fix into $branch (next rc)" --force

cat <<EOF

cut complete
  branch : $branch @ ${sha:0:12}
  tag    : $tag  (release.yaml rc path is now running)
  label  : backport $branch
EOF
