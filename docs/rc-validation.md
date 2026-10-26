# RC validation through RNPM

After the release kit publishes an RC, `release.yaml` calls
`POST /api/bundles/{id}/trigger` and waits for RNPM's existing OB verdict and
delivery of `rc-validation` to the RC commit. It does not rebuild the RC image
or implement a separate test-report verdict.

## Setup

- GitHub Actions secret `RNPM_TOKEN`: an RNPM bearer token whose owner has
  `builds.run` and `integrations.write`. RNPM's GitHub integration must be able
  to write commit statuses to `RBLN-SW/rbln-npu-operator`.
- Variable `RNPM_URL` defaults to `https://rnpm.k8s.rebellions.in`.
- `.github/scripts/rnpm-rc.json` records bundle `c2b388aa-6b89-4337-9521-01501f0a1998` and OB
  pipeline `scheduled-jigsaw-npu-operator-matrix-validation`. `RNPM_RC_BUNDLE_ID` can override the ID;
  the helper verifies the execution repository and OB link.
- `CLOUD_COMPONENT_HARBOR_REGISTRY` is the same host/project used by the release
  kit to publish images. Registry credentials remain managed by RNPM/OB.
- Merge the bootstrap into the branch/commit OB will check out, sync that
  repository in RNPM, and switch the OB entry point to that bootstrap.
  It must forward `?run=$RNPM_RUN`. The helper fails before triggering if the
  recorded bootstrap or OB entry point is not ready.
- Release branches need this workflow, helper and bootstrap before creating a
  new RC. An old RC tag still contains its old workflow.
- `RC_VALIDATION_SENTINEL_REF` optionally overrides the harness ref on every
  member; sync that ref's catalog in RNPM first.
- `RNPM_VALIDATE_BRANCH` (fallback: existing `BUILDKITE_VALIDATE_BRANCH`) and
  `RNPM_VALIDATE_COMMIT` optionally pin the test-infra checkout.
- `SENTINEL_CATALOG_IMAGE` is required and supplied by `rc-olm-check`.

## Request contract

`build.enabled=false` disables both the build step and commit-SHA image-tag
rewriting. `force_env=true` applies this RC's explicit image values over saved
member Env. `commit_status={enabled:true, context:"rc-validation"}` and
`status_target={repository, sha}` let RNPM report directly to the release
commit. Saved PR/nightly definitions and other runs are unchanged.

The Operator RC uses the registered test-infra Matrix bundle. The execution
repository is `rebellions-sw/npu-operator-test-infra`, normally `dev`/`HEAD`;
the status target remains this Operator repository and the exact RC SHA.

`SENTINEL_HELM_CHART` and `SENTINEL_HELM_CHART_VERSION` select the published
Harbor OCI chart. Every member receives an explicit OCI chart override so
a saved clone/repo chart cannot win. `SENTINEL_CATALOG_IMAGE` comes from the
successful OLM publication job. Operator/validator and operand image overrides
are cleared so the published chart chooses its image combination. Kernel
driver, SMD, compiler and cluster-specific settings are preserved.

The helper also attempts to cancel active older RCs of the same version through
RNPM. Grant `builds.control` for this optional operation; a denial is a warning
and validation continues. It leaves the same/newer RCs and unrelated runs alone.

## Result and retries

The trigger's HTTP 201 means a build was created. The helper records its build
URL and run ID, then reads that exact build/run until RNPM has delivered the
matching final `rc-validation` status. Success exits zero; the existing OB
failure/error result exits nonzero. Cleanup/notification failures are part of
the normal RNPM/OB result. No coverage-count or custom RC verdict is added.

A trigger request is sent once. On an ambiguous timeout, inspect RNPM Runs
before retrying; a repeated POST creates another build. A wait timeout does
not cancel the OB build. Use RNPM to inspect, cancel or retry an existing run;
do not rerun image publication just to retry validation.

For a read-only payload check with the same release environment:
`python3 .github/scripts/rnpm_rc.py --dry-run`. This reads RNPM configuration
and calls preview only. The offline unittest suite intercepts all requests.
