#!/usr/bin/env bash
# Shared helpers for the operator-specific release scripts. Source, do not
# execute. The release process itself (cut, tag, classify, policy, notes,
# promotion) lives in the release kit:
# https://github.com/RBLN-SW/cloud-component-release-kit

fail() {
	echo "ERROR: $*" >&2
	exit 1
}

note() { echo "== $*"; }
