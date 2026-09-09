#!/usr/bin/env bash
# Shared helpers for the release scripts. Source, do not execute.
#
# Conventions the scripts rely on:
#   - Release branches are named release-X.Y.Z and are cut from main.
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

# Tally Conventional Commit subjects in a revision range and suggest the next
# version relative to the last GA. Informational only: the release manager
# picks the number.
summarize_commits() {
	local range=$1 last=${2:-}
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

	[ -n "$last" ] || return 0
	local base=${last#v} x y z
	IFS=. read -r x y z <<<"$base"
	local suggestion
	if [ "$breaking" -gt 0 ] && [ "$x" -gt 0 ]; then
		suggestion="$((x + 1)).0.0"
	elif [ "$feat" -gt 0 ] || [ "$breaking" -gt 0 ]; then
		suggestion="$x.$((y + 1)).0"
	else
		suggestion="$x.$y.$((z + 1))"
	fi
	echo "   suggested next version (semver, informational): $suggestion"
}
