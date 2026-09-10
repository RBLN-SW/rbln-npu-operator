#!/usr/bin/env bash
# Shared helpers for the release scripts. Source, do not execute.
#
# Conventions the scripts rely on:
#   - Release branches are named release-X.Y.Z. A minor or major is cut from
#     main; a patch is cut from the tag of the newest GA.
#   - Release candidates are tagged vX.Y.Z-rcN, GA is tagged vX.Y.Z, both on
#     the release branch only. main never carries a version tag.
#   - The remote is called origin.

fail() {
	echo "ERROR: $*" >&2
	exit 1
}

note() { echo "== $*"; }

# Newest GA tag (vX.Y.Z, no -rc suffix) known locally. Empty if none.
last_ga_tag() {
	git tag -l 'v[0-9]*' | grep -Ev -- '-rc[0-9]+$' | sort -V | tail -1
}

# Newest GA tag strictly lower than the given vX.Y.Z. Empty if none.
prev_ga_tag() {
	local cur=$1
	{
		git tag -l 'v[0-9]*' | grep -Ev -- '-rc[0-9]+$'
		echo "$cur"
	} | sort -Vu | awk -v cur="$cur" '$0 == cur { print prev; exit } { prev = $0 }'
}

# Highest rcN tag for version X.Y.Z (argument without the v). Empty if none.
last_rc_tag() {
	git tag -l "v$1-rc[0-9]*" | sort -V | tail -1
}

# Version a cut of the given kind produces, relative to the newest GA tag.
#   minor -> X.(Y+1).0    patch -> X.Y.(Z+1)    major -> (X+1).0.0
next_version() { # <minor|patch|major> <vX.Y.Z>
	local kind=$1 base=${2#v} x y z
	IFS=. read -r x y z <<<"$base"
	case $kind in
	major) echo "$((x + 1)).0.0" ;;
	minor) echo "$x.$((y + 1)).0" ;;
	patch) echo "$x.$y.$((z + 1))" ;;
	*) return 1 ;;
	esac
}

# State of one context in a commit's combined status: success, failure, error,
# pending, or "none" when the context was never reported. Needs gh + GH_TOKEN.
commit_status_state() { # <sha> <context>
	local repo=${GITHUB_REPOSITORY:-rebellions-sw/rbln-npu-operator} state
	state=$(gh api "repos/$repo/commits/$1/status" 2>/dev/null |
		jq -r --arg c "$2" '[.statuses[] | select(.context == $c) | .state][0] // "none"' 2>/dev/null) ||
		state=none
	echo "${state:-none}"
}

# Tally Conventional Commit subjects in a revision range. With a cut kind, warn
# when the tally does not match what that kind is supposed to carry. The kind
# decides the number; this is informational only.
summarize_commits() { # <range> [<minor|patch|major>]
	local range=$1 kind=${2:-}
	local breaking=0 feat=0 fix=0 other=0 subj
	# Regexes live in variables: bash cannot parse parentheses inside [[ =~ ]].
	local re_breaking='^[a-z]+(\([^)]*\))?!:' re_feat='^feat(\([^)]*\))?:' re_fix='^fix(\([^)]*\))?:'
	while IFS= read -r subj; do
		if [[ $subj =~ $re_breaking ]]; then
			breaking=$((breaking + 1))
		elif [[ $subj =~ $re_feat ]]; then
			feat=$((feat + 1))
		elif [[ $subj =~ $re_fix ]]; then
			fix=$((fix + 1))
		else
			other=$((other + 1))
		fi
	done < <(git log --format=%s "$range")
	echo "   breaking: $breaking  feat: $feat  fix: $fix  other: $other"

	case $kind in
	minor)
		if [ "$breaking" -eq 0 ] && [ "$feat" -eq 0 ]; then
			echo "   note: no feat or breaking commits since the last GA; a cut from main is still a minor"
		fi
		;;
	patch)
		if [ "$breaking" -gt 0 ] || [ "$feat" -gt 0 ]; then
			echo "   warn: a patch carries fixes only, but the range contains feat or breaking commits"
		fi
		;;
	esac
}
