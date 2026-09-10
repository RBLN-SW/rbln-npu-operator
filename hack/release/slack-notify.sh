#!/usr/bin/env bash
# Post a one-line message to the release Slack channel.
#
#   hack/release/slack-notify.sh "<text>"
#
# Uses the same bot token as .github/workflows/nightly.yaml (SLACK_OAUTH_TOKEN)
# and SLACK_CHANNEL_ID. A missing token or a Slack error is reported as a
# workflow warning and never fails the calling job: a lost notification must
# not block a release.

set -euo pipefail

text=${*:?usage: slack-notify.sh <text>}
if [ -z "${SLACK_OAUTH_TOKEN:-}" ]; then
	echo "::notice::SLACK_OAUTH_TOKEN not set; skipping Slack: $text"
	exit 0
fi
channel=${SLACK_CHANNEL_ID:?SLACK_CHANNEL_ID is required when SLACK_OAUTH_TOKEN is set}

if [ -n "${GITHUB_SERVER_URL:-}" ] && [ -n "${GITHUB_RUN_ID:-}" ]; then
	text="$text (<$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID|run>)"
fi

resp=$(curl -sS -X POST https://slack.com/api/chat.postMessage \
	-H "Authorization: Bearer $SLACK_OAUTH_TOKEN" \
	-H 'Content-type: application/json; charset=utf-8' \
	--data "$(jq -n --arg c "$channel" --arg t "$text" '{channel: $c, text: $t, unfurl_links: false}')") || {
	echo "::warning::Slack request failed"
	exit 0
}
if ! jq -e '.ok' >/dev/null <<<"$resp"; then
	echo "::warning::Slack rejected the message: $(jq -r '.error // "unknown"' <<<"$resp")"
fi
