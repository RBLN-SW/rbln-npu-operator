# Security policy

## Supported versions

Only the **latest GA release** of rbln-npu-operator is supported. Every
release is cut from `main`, so a fix for any earlier version ships as the next
release: either the next monthly release or, for a blocker, a hotfix release
(`X.Y.Z+1`) cut from `main` and soaked briefly. Older releases do not receive
patches; upgrade to the latest release to obtain fixes.

The release process, including how a fix reaches a release, is described in
[docs/release-process.md](docs/release-process.md).

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Use GitHub's private vulnerability reporting on this repository
(**Security → Report a vulnerability**). If that is not available to you,
contact the maintainers through your Rebellions support channel.

You should receive an acknowledgement within a few business days. We will
work with you on a fix and a disclosure timeline. Embargoed fixes are
developed in the advisory's temporary private fork and merged to `main` when
the advisory is published; the fix then ships through the normal release
process, as a hotfix if warranted.

## Scope

This policy covers the operator, its validator image and the Helm chart in
this repository. Component images the operator deploys (device plugin,
metrics exporter, container toolkit, driver manager and others) have their
own repositories; a report about one of them is forwarded to that project.
Dependency advisories are handled continuously by the nightly image scan and
do not require a report.
