#!/usr/bin/env bash
# Post a message to the release Slack channel.
#
#   hack/release/slack-notify.sh "<text>"
#   hack/release/slack-notify.sh --color <good|warning|danger|info|#hex> \
#       --title "<mrkdwn>" [--details "<mrkdwn>"] [--link "<url>|<label>"]... \
#       "<body mrkdwn, may span lines, may be empty>"
#
# The one-argument form posts plain text (with the run link appended), as
# before. The flagged form posts a coloured attachment in the same shape as the
# nightly messages: a bold title, the body, an optional details block and a
# context line with the workflow run and any extra links. Bodies are mrkdwn,
# so `code`, *bold* and bullets (•) render.
#
# --details is a context block: Slack renders it in the small font with small
# emoji, the way the nightly matrix report lists its scenarios. Status rows
# (":white_check_mark: Images ...") belong there; a regular section would show
# every emoji at full size. Use emoji shortcodes, not unicode, for the same
# rendering as that report.
#
# The plain-text preview for notifications goes in the attachment's fallback,
# not the message's top-level text: Slack renders top-level text as a body
# above the attachment, which showed the title twice.
#
# Uses the same bot token as .github/workflows/nightly.yaml (SLACK_OAUTH_TOKEN)
# and SLACK_CHANNEL_ID. A missing token or a Slack error is reported as a
# workflow warning and never fails the calling job: a lost notification must
# not block a release.

set -euo pipefail

color="" title="" details="" links=()
while [ $# -gt 1 ]; do
	case $1 in
	--color) color=$2; shift 2 ;;
	--title) title=$2; shift 2 ;;
	--details) details=$2; shift 2 ;;
	--link) links+=("$2"); shift 2 ;;
	*) break ;;
	esac
done
if [ $# -ne 1 ]; then
	echo 'usage: slack-notify.sh [--color c --title t [--details d] [--link "url|label"]...] <text>' >&2
	exit 2
fi
body=$1

if [ -z "${SLACK_OAUTH_TOKEN:-}" ]; then
	echo "::notice::SLACK_OAUTH_TOKEN not set; skipping Slack: ${title:+$title — }$body"
	exit 0
fi
channel=${SLACK_CHANNEL_ID:?SLACK_CHANNEL_ID is required when SLACK_OAUTH_TOKEN is set}

run_link=""
if [ -n "${GITHUB_SERVER_URL:-}" ] && [ -n "${GITHUB_RUN_ID:-}" ]; then
	run_link="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
fi

case $color in
good) color="#36a64f" ;;
warning) color="#FFA500" ;;
danger) color="#FF0000" ;;
info) color="#439FE0" ;;
esac

if [ -z "$title" ]; then
	# Plain text, as before.
	text=$body
	[ -n "$run_link" ] && text="$text (<$run_link|run>)"
	payload=$(jq -n --arg c "$channel" --arg t "$text" '{channel: $c, text: $t, unfurl_links: false}')
else
	context=()
	[ -n "$run_link" ] && context+=("<$run_link|workflow run>")
	for l in ${links[@]+"${links[@]}"}; do context+=("<${l%%|*}|${l#*|}>"); done
	[ -n "${GITHUB_ACTOR:-}" ] && context+=("by ${GITHUB_ACTOR}")
	ctx=$(IFS=' '; printf '%s' "${context[*]/%/  ·}" | sed 's/  ·$//')
	fallback=$(printf '%s' "$title" | sed -E 's/:[a-z0-9_+-]+: ?//g; s/[*`]//g')
	payload=$(jq -n --arg c "$channel" --arg fb "$fallback" --arg color "$color" \
		--arg title "$title" --arg body "$body" --arg details "$details" --arg ctx "$ctx" '
		{channel: $c, unfurl_links: false,
		 attachments: [{fallback: $fb, color: $color, blocks: (
		   [{type: "section", text: {type: "mrkdwn", text: $title}}]
		   + (if $body == "" then [] else [{type: "section", text: {type: "mrkdwn", text: $body}}] end)
		   + (if $details == "" then [] else [{type: "context", elements: [{type: "mrkdwn", text: $details}]}] end)
		   + (if $ctx == "" then [] else [{type: "context", elements: [{type: "mrkdwn", text: $ctx}]}] end))}]}')
fi

resp=$(curl -sS -X POST https://slack.com/api/chat.postMessage \
	-H "Authorization: Bearer $SLACK_OAUTH_TOKEN" \
	-H 'Content-type: application/json; charset=utf-8' \
	--data "$payload") || {
	echo "::warning::Slack request failed"
	exit 0
}
if ! jq -e '.ok' >/dev/null <<<"$resp"; then
	echo "::warning::Slack rejected the message: $(jq -r '.error // "unknown"' <<<"$resp")"
fi
