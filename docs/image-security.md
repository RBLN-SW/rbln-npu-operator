# Container image security

Every pull request builds the container images this repo publishes and scans
them with [Trivy](https://trivy.dev) for OS-package vulnerabilities, Go module
and stdlib vulnerabilities, and secrets accidentally baked into a layer. A
nightly run repeats the scan against `main` and reports to Slack.

## What is scanned

| image | Dockerfile | base |
| --- | --- | --- |
| `rbln-npu-operator` | `Dockerfile` | `redhat/ubi9-minimal` |
| `rbln-vfio-manager` | `images/vfio-manager/Dockerfile` | `redhat/ubi9` |
| `rbln-node-reboot` | `images/node-reboot/Dockerfile` | `alpine` |

The operator image carries three binaries — `npu-operator`, `rbln-validator`
and `crd-apply` — each analysed separately for Go dependencies.

### What is not scanned, and why

- **`rbln-npu-operator-validator`** is built from the same `Dockerfile` as the
  operator image with only a different tag, so its content is byte-identical.
  Scanning it would duplicate the operator leg exactly.
- **`rbln-npu-operator-bundle`** is `FROM scratch` — manifests and labels only,
  with no packages or binaries to analyse.
- **`rbln-npu-operator-catalog`** is built `FROM quay.io/operator-framework/opm`.
  Its findings are upstream and can only be changed here by moving
  `OPM_VERSION`, which is constrained by the certified-operators FBC pipeline.
  Gating on an image we cannot patch would produce an unactionable red. Worth
  re-measuring whenever `OPM_VERSION` moves.

## What fails the build

A finding fails the gate when it is **HIGH or CRITICAL** *and* **fixable** —
that is, the advisory names a fixed version. Secrets fail at HIGH/CRITICAL
regardless.

Unfixable findings are excluded on purpose: there is no action available for
them, and failing on them trains reviewers to ignore the gate. They stay
visible in the SARIF upload and the repository's security tab.

## Reproducing a failure locally

```bash
make scan-images
```

This builds all three images and scans them with the same severity, scanner
set, `--ignore-unfixed` behaviour and ignore file as CI. Install Trivy first;
the pinned version is `TRIVY_VERSION` in `versions.mk`.

To narrow the scan to one image, override `TRIVY_IMAGES`:

```bash
make scan-images TRIVY_IMAGES=docker.io/rebellions/rbln-npu-operator:v0.4.4
```

Note that `scan-images` builds all three images first regardless; override
`TRIVY_IMAGES` only to narrow what gets scanned.

## Fixing a finding

Work through these in order — the cheapest fix that clears the finding is
almost always the right one.

1. **OS package** → bump the base image tag in the relevant Dockerfile. Red Hat
   ships fixes in UBI patch releases, so `9.6` → `9.8` typically clears the
   whole OS set at once.
2. **Go `stdlib`** → bump `GOLANG_VERSION` in `versions.mk` **and** the
   `ARG GOLANG_VERSION` default in `Dockerfile`. These must move together;
   `make verify-versions` enforces it. No API risk.
3. **Go module** → bump the module. Read the next section first: in this repo a
   module bump can be constrained by the Kubernetes and operator-framework
   version matrix.
4. **Not fixable here** → record an exception (see below).

## Dependency upgrades and the compatibility matrix

This is an operator, so a dependency bump is not just a dependency bump. The
pins below move together, and `make verify-versions` enforces the parts that
are mechanically unambiguous.

### Kubernetes libraries

Everything in the `k8s.io/*` family must share one minor:
`k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`,
`k8s.io/apiextensions-apiserver`, `k8s.io/apiserver`, `k8s.io/kubectl`,
`k8s.io/component-base`, `k8s.io/cli-runtime`.

`k8s.io/klog/v2`, `k8s.io/utils` and `k8s.io/kube-openapi` are exempt — they do
not follow the `v0.<k8s-minor>.<patch>` scheme.

Bumping one of these alone is the most common way a CVE-driven `go get` breaks
the build, so it is a hard check.

### controller-runtime and envtest

`sigs.k8s.io/controller-runtime` and `ENVTEST_VERSION` are released in lockstep:
controller-runtime `v0.22.x` pairs with `ENVTEST_VERSION=release-0.22`. Moving
one without the other makes envtest download test binaries from the wrong
branch. This is a hard check.

### The known off-matrix pairing

The repo currently runs **controller-runtime v0.22.4** (which targets Kubernetes
**1.34**) against **`k8s.io/*` v0.35.1** (Kubernetes **1.35**), with
`ENVTEST_K8S_VERSION=1.34.1` matching controller-runtime rather than the
libraries.

This is known and intentional. It is *not* checked automatically, because
`ENVTEST_K8S_VERSION=1.34.1` is correct for controller-runtime 0.22 — the
apparent skew is against the libraries, not against the test harness.

Resolving it properly means moving to **controller-runtime v0.23** (targets
k8s 1.35) together with `ENVTEST_VERSION=release-0.23` and
`ENVTEST_K8S_VERSION=1.35.x`, and absorbing the controller-runtime API changes
across that minor. That is a judgment call, not a rule, so it is documented
here rather than encoded as a check that would fire on every PR with no cheap
resolution.

### Helm

`helm.sh/helm/v3` tracks one Kubernetes minor per Helm minor (`v3.20.x` pins
`k8s.io/*` v0.35.x) and pulls a registry client stack — `containerd`,
`oras-go` — into the tree. Keep it on the Helm minor whose `k8s.io/*` pin
matches the family above: moving Helm alone drags `client-go` to another minor
and trips the skew check; moving `k8s.io/*` alone leaves Helm pinning the old
one. When an advisory lands in Helm's registry stack, bump the affected module
directly first (`containerd` moves freely within `v1.7.x`); a Helm bump is the
fallback. Helm is only imported by the e2e suite, so it never reaches the
shipped binaries, but Trivy and govulncheck still see it in the module graph.

### go-containerregistry

`github.com/google/go-containerregistry` is what pins `github.com/docker/cli`
(it parses Docker credential-helper config). It has no Kubernetes coupling, so
bump it directly when a `docker/cli` advisory lands rather than pinning
`docker/cli` by hand.

### operator-framework tooling

- `OPERATOR_SDK_VERSION` is stamped into `bundle.Dockerfile` as the
  `operators.operatorframework.io.metrics.builder` label. After bumping it, run
  `make bundle` so the label and the generated CSV agree.
- `CONTROLLER_TOOLS_VERSION` drives CRD and DeepCopy generation. After bumping
  it, run `make manifests generate sync-crds` and check the CRD diff — a
  controller-gen bump can change generated schemas. `make verify-manifests-sync`
  fails on unsynced output.
- `OPM_VERSION` affects the rendered FBC catalog and is aligned with the
  certified-operators FBC Makefile default. Changing it affects what ships to
  Red Hat.

After any dependency bump, `make code-check`, `make unit-tests`,
`make bundle` and `make catalog` all need to pass. They run on every PR.

## Recording an exception

When a finding genuinely cannot be fixed here, add it to `.trivyignore.yaml`:

```yaml
vulnerabilities:
  - id: CVE-2026-00000
    statement: >-
      Why this is not actionable in this repo, and what would unblock it.
    expired_at: 2026-12-31
```

Both fields are mandatory by convention:

- **`statement`** must name what blocks the real fix, so the next reader does
  not have to re-derive it.
- **`expired_at`** is the enforcement mechanism. Trivy re-surfaces the finding
  once the date passes, turning the gate red and forcing a re-review. An
  exception cannot rot silently.

Keep expiry windows short — a quarter is usually right. An exception is a note
to revisit, not a permanent waiver.

## CI layout

- `.github/workflows/image-scan.yaml` — reusable; builds and scans the three
  images in a matrix, uploads SARIF per image, aggregates counts and applies
  the gate.
- `.github/workflows/trigger-pr.yaml` — calls it with `fail-on-findings: true`.
- `.github/workflows/nightly.yaml` — 03:00 KST, rebuilds `main` from scratch so
  base-image drift surfaces without a code change, and posts to Slack on every
  run including clean ones. Silence means the nightly stopped running, not that
  the images are healthy.

The nightly needs the `SLACK_OAUTH_TOKEN` repository secret and posts to the
channel in the `SLACK_CHANNEL_ID` repository variable.
