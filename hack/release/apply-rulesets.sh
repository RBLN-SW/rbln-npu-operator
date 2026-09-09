#!/usr/bin/env bash
# Apply the release rulesets in .github/rulesets/ to the repository.
#
#   hack/release/apply-rulesets.sh --team <team-slug> --app-id <github-app-id> [--repo <owner/repo>] [--dry-run]
#
# Rulesets are kept as JSON in the repo so the same protection can be reviewed
# in a PR and replayed on other repos. __TEAM_ID__ and __APP_ID__ placeholders
# are resolved here: the team slug becomes its numeric id, the App id is the
# GitHub App's id (Settings > Developer settings > GitHub Apps), not the
# installation id.
#
# An existing ruleset with the same name is updated in place (PUT), otherwise
# it is created (POST). If the API rejects the `required_reviewers` parameter
# (its schema is newer than some GHES/API versions), the ruleset is applied
# again without it and a warning tells you to require the team via CODEOWNERS
# on the release branch instead.
#
# Requires: gh authenticated with admin rights on the repository.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

team="" app_id="" repo="" dry_run=0
while [ $# -gt 0 ]; do
	case $1 in
	--team) team=$2; shift 2 ;;
	--app-id) app_id=$2; shift 2 ;;
	--repo) repo=$2; shift 2 ;;
	--dry-run) dry_run=1; shift ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
[ -n "$team" ] && [ -n "$app_id" ] || { echo "usage: apply-rulesets.sh --team <slug> --app-id <id> [--repo owner/repo] [--dry-run]" >&2; exit 2; }
[ -n "$repo" ] || repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
org=${repo%%/*}

team_id=$(gh api "orgs/$org/teams/$team" --jq .id) || { echo "team $org/$team not found" >&2; exit 1; }
echo "repo=$repo team=$team (id $team_id) app_id=$app_id"

existing=$(gh api "repos/$repo/rulesets" --jq '.[] | "\(.name) \(.id)"')

apply() { # <name> <json>
	local name=$1 body=$2 id method path
	id=$(awk -v n="$name" '$1 == n { print $2 }' <<<"$existing")
	if [ -n "$id" ]; then method=PUT; path="repos/$repo/rulesets/$id"; else method=POST; path="repos/$repo/rulesets"; fi
	if [ "$dry_run" = 1 ]; then
		echo "--- $method $path"; jq . <<<"$body"; return 0
	fi
	gh api -X "$method" "$path" --input - <<<"$body" >/dev/null
}

for f in .github/rulesets/*.json; do
	name=$(jq -r .name "$f")
	body=$(sed -e "s/\"__TEAM_ID__\"/$team_id/g" -e "s/\"__APP_ID__\"/$app_id/g" "$f")
	if apply "$name" "$body"; then
		echo "ok    $name"
		continue
	fi
	if jq -e '.rules[] | select(.type == "pull_request") | .parameters.required_reviewers' <<<"$body" >/dev/null 2>&1; then
		echo "warn  $name: retrying without pull_request.required_reviewers"
		body=$(jq '(.rules[] | select(.type == "pull_request") | .parameters) |= del(.required_reviewers)' <<<"$body")
		if apply "$name" "$body"; then
			echo "ok    $name (without required_reviewers). Add '* @$org/$team' to .github/CODEOWNERS on release branches and set require_code_owner_review, or upgrade the API."
			continue
		fi
	fi
	echo "FAIL  $name" >&2
	exit 1
done
