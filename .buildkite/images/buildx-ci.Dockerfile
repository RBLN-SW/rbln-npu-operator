# Main container image for the Buildkite `build-image` step (DinD pattern).
#
# Base: alpine/docker-with-buildx — provides docker CLI + buildx.
# Added: bash (Buildkite agent defaults to /bin/bash -e -c), make (Makefile
# targets), git (sentinel clone + buildkite-agent git helpers).
FROM alpine/docker-with-buildx:latest

RUN apk add --no-cache bash make git
