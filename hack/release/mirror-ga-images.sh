#!/usr/bin/env bash
# Keep the operator's GA image tags in Harbor after the release kit promotes
# the validated RC to Docker Hub. Copy the exact promoted digests, not a
# mutable RC tag. Required env: HARBOR_REGISTRY, PROMOTED (image=digest lines).
set -euo pipefail
# shellcheck source=hack/release/lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

tag=${1:?usage: mirror-ga-images.sh <vX.Y.Z>}
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "expected a GA tag, got $tag"
: "${HARBOR_REGISTRY:?}" "${PROMOTED:?}"
registry=${HARBOR_REGISTRY%/}
images=(rbln-npu-operator rbln-npu-operator-validator)
digests=()
present=()

# Validate both sources and destinations before writing either GA tag.
for image in "${images[@]}"; do
	digest=$(awk -F= -v image="$image" '$1 == image {print $2}' <<< "$PROMOTED")
	[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || fail "invalid or missing promoted digest for $image"
	source="$registry/$image@$digest"
	actual=$(regctl image digest "$source")
	[ "$actual" = "$digest" ] || fail "source digest mismatch for $source: $actual"

	# Listing tags distinguishes an absent tag from authentication/network errors.
	tags=$(regctl tag ls "$registry/$image")
	exists=false
	if grep -Fxq -- "$tag" <<< "$tags"; then
		actual=$(regctl image digest "$registry/$image:$tag")
		[ "$actual" = "$digest" ] || fail "$registry/$image:$tag already points to a different digest"
		exists=true
	fi
	digests+=("$digest")
	present+=("$exists")
done

for i in "${!images[@]}"; do
	target="$registry/${images[$i]}:$tag"
	if [ "${present[$i]}" = false ]; then
		regctl image copy "$registry/${images[$i]}@${digests[$i]}" "$target"
	fi
	actual=$(regctl image digest "$target")
	[ "$actual" = "${digests[$i]}" ] || fail "digest changed while copying $target: $actual"
	note "$target@$actual"
done
