#!/usr/bin/env bash
# Create the static labels the release process uses. Idempotent.
#
#   hack/release/setup-labels.sh [<owner/repo>]
#
# The per-release label "backport release-X.Y.Z" is not created here: the
# cut-release workflow creates it at cut and the GA workflow deletes it, which
# is what makes "no backports after GA" a structural property rather than a
# rule to remember.

set -euo pipefail

repo=${1:-${GITHUB_REPOSITORY:-}}
args=()
[ -n "$repo" ] && args=(--repo "$repo")

label() { # <name> <color> <description>
	gh label create "$1" --color "$2" --description "$3" --force "${args[@]}"
	echo "ok    $1"
}

# GitHub caps label descriptions at 100 characters.
label backport-manual 5319E7 "Hand-resolved cherry-pick; content may differ from the main commit (release-policy accepts)"
label release-only FBCA04 "No counterpart on main; PR body must say why. Release manager approval required"
label release-blocker B60205 "Candidate for the rc after the final-rc announcement; taking it delays GA. Priority only"
label tag-rc 0052CC "When merged into a release branch, tag-release tags the next rc. Put it on the last PR of a batch"
label ci/full-e2e 1D76DB "PR CI runs the nightly's full container-track scenario list instead of the base set"

# release-policy probe: this line only exists to exercise the check on a PR into a release branch.
