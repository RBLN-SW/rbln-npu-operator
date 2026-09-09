#!/usr/bin/env bash
# Policy checks for pull requests whose base is a release-* branch.
#
# Everything on a release branch must be a fix that already lives on main
# (trunk-first), carried over unchanged. This script proves that mechanically.
#
#   #  check                       fails when                                   effect
#   1  title type                  type not in fix|revert|docs|test|ci|build|   block
#                                  chore, or a breaking "!" marker
#   2  no merge commits            base..head contains a merge                   block
#   3  cherry-pick trailer         a commit lacks "(cherry picked from commit    block (skipped with release-only)
#                                  <sha>)"
#   4  origin is on main           trailer sha unknown or not an ancestor of     block (skipped with release-only)
#                                  origin/main
#   5  content equals origin       git patch-id differs from the origin commit   block unless labelled backport-manual
#   6  release-only rationale      release-only label without a "why not main"   block
#                                  section in the PR body
#   7  size                        >400 changed lines outside generated paths    comment only
#
# Inputs (environment): BASE_REF PR_NUMBER PR_TITLE PR_BODY PR_LABELS (newline
# separated) GITHUB_REPOSITORY GH_TOKEN. HEAD is the PR head checkout.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

: "${BASE_REF:?}" "${PR_NUMBER:?}" "${PR_TITLE:?}"
PR_BODY=${PR_BODY:-}
PR_LABELS=${PR_LABELS:-}
if [ -n "${PR_LABELS_JSON:-}" ]; then # workflow passes labels as a JSON array
	PR_LABELS=$(jq -r '.[]' <<<"$PR_LABELS_JSON")
fi
SIZE_LIMIT=${SIZE_LIMIT:-400}
ALLOWED_TYPES='fix|revert|docs|test|ci|build|chore'
GENERATED_PATHS=(':!config/crd/bases' ':!bundle' ':!vendor' ':!deployments/rbln-npu-operator/crds' ':!*zz_generated*' ':!go.sum')

failures=()
problem() {
	failures+=("$*")
	echo "FAIL  $*"
}
pass() { echo "ok    $*"; }
has_label() { grep -qx -- "$1" <<<"$PR_LABELS"; }

git fetch --quiet origin main "refs/heads/$BASE_REF:refs/remotes/origin/$BASE_REF"
base="origin/$BASE_REF"

# 1. title type (regexes in variables: bash cannot parse parentheses inside [[ =~ ]])
re_allowed="^($ALLOWED_TYPES)(\([^)]+\))?!?: "
re_breaking='^[a-z]+(\([^)]+\))?!:'
if [[ $PR_TITLE =~ $re_allowed ]]; then
	if [[ $PR_TITLE =~ $re_breaking ]]; then
		problem "title: breaking change marker '!' is not allowed on a release branch"
	else
		pass "title type allowed"
	fi
else
	problem "title: type must be one of ${ALLOWED_TYPES//|/, } (got '${PR_TITLE%%:*}')"
fi

# 2. no merge commits
if [ -n "$(git rev-list --merges "$base..HEAD")" ]; then
	problem "merge commits are not allowed (squash-only history)"
else
	pass "no merge commits"
fi

# 3-5. provenance
commits=$(git rev-list --reverse "$base..HEAD")
if has_label release-only; then
	echo "skip  provenance checks (release-only)"
else
	for c in $commits; do
		short=${c:0:12}
		origins=$(git log -1 --format=%B "$c" |
			sed -n 's/^(cherry picked from commit \([0-9a-f]\{40\}\))$/\1/p')
		if [ -z "$origins" ]; then
			problem "$short: no '(cherry picked from commit <sha>)' trailer. Cherry-pick with -x, or label release-only with a rationale."
			continue
		fi
		for o in $origins; do
			if ! git cat-file -e "$o^{commit}" 2>/dev/null; then
				problem "$short: origin $o is not a commit in this repository"
				continue
			fi
			if ! git merge-base --is-ancestor "$o" origin/main; then
				problem "$short: origin ${o:0:12} is not on main (trunk-first: land it on main first)"
				continue
			fi
			pid_c=$(git show "$c" | git patch-id --stable | cut -d' ' -f1)
			pid_o=$(git show "$o" | git patch-id --stable | cut -d' ' -f1)
			if [ "$pid_c" = "$pid_o" ]; then
				pass "$short: identical to main commit ${o:0:12}"
			elif has_label backport-manual; then
				pass "$short: differs from ${o:0:12} (accepted: backport-manual)"
			else
				problem "$short: content differs from origin ${o:0:12}. If you resolved a conflict by hand, add the backport-manual label."
			fi
		done
	done
fi

# 6. release-only rationale
if has_label release-only; then
	if grep -qiE 'why not main|main에 (왜 )?불필요' <<<"$PR_BODY"; then
		pass "release-only: rationale present"
	else
		problem "release-only: PR body must contain a 'Why not main' section explaining why this change has no counterpart on main"
	fi
fi

# 7. size (advisory)
changed=$(git diff --numstat "$base...HEAD" -- . "${GENERATED_PATHS[@]}" |
	awk '{ a += ($1 == "-" ? 0 : $1); d += ($2 == "-" ? 0 : $2) } END { print a + d + 0 }')
if [ "$changed" -gt "$SIZE_LIMIT" ]; then
	echo "warn  $changed changed lines outside generated paths (limit $SIZE_LIMIT)"
	marker='<!-- release-policy:size -->'
	if [ -n "${GH_TOKEN:-}" ] && ! gh api "repos/$GITHUB_REPOSITORY/issues/$PR_NUMBER/comments" --jq '.[].body' 2>/dev/null | grep -q "$marker"; then
		gh pr comment "$PR_NUMBER" --body "$marker
**release-policy**: this PR changes $changed lines outside generated paths (advisory limit $SIZE_LIMIT). Large backports raise the soak risk; make sure this is a fix, not a feature riding along." || true
	fi
else
	pass "size: $changed changed lines outside generated paths"
fi

if [ ${#failures[@]} -gt 0 ]; then
	echo
	echo "release-policy: ${#failures[@]} problem(s)"
	exit 1
fi
echo
echo "release-policy: all checks passed"
