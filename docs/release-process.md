# Release process

How a version of rbln-npu-operator goes from `main` to docker.io, the Helm OCI
registry and the Red Hat certified-operators catalog, who does what along the
way, and how the repository has to be configured for it. The design record
behind these choices lives in `docs/superpowers/specs/` (local only).

## Principles

1. **`main` is always open and always releasable.** There is no code freeze.
   Unfinished work stays on a feature branch or behind a default-off flag.
2. **A release is cut by kind, not by number.** `minor` cuts
   `release-X.(Y+1).0` from `main` and carries everything merged since the
   last GA; while the major is 0, breaking changes ship here too and are
   called out in the notes. `patch` cuts `release-X.Y.(Z+1)` from the tag of
   the newest GA and carries only fixes that already live on `main`. `major`
   is an explicit decision (`X+1` from `main`). The branch name is the
   version; the tooling computes it.
3. **Only fixes that already live on `main` enter a release branch**, via a
   label that makes a bot cherry-pick them. Only the release-manager team can
   merge there.
4. **Tags drive everything.** `vX.Y.Z-rcN` publishes a release candidate to
   the internal registry and hands it to the test-infra matrix for
   validation. `vX.Y.Z` promotes the newest rc by digest to docker.io,
   publishes the chart and opens the OLM bundle PR. GA is refused unless it is
   the exact commit of the newest rc **and** that commit carries a passing
   `rc-validation` status.
5. **The tooling knows no dates.** "Monthly, last Friday" is a team habit the
   release manager keeps in their head; the process reads its state from
   branches, tags, labels and commit statuses.
6. **Only the newest GA is supported.** Bugs are fixed on `main` and shipped
   as the next cut. A fix users of the current GA cannot wait for goes out as
   a `patch` cut from that GA's tag. A patch for an older GA is refused: the
   OLM upgrade graph must stay linear. A fix is backported to every open
   release branch, and an open patch goes GA before the next minor does.

## States

| State | How you tell | What is allowed |
|---|---|---|
| open | no unreleased `release-*` branch | anything on `main` |
| validating | `release-X.Y.Z` has rc tags but no GA tag | labelled fix backports, each producing a new rc; every rc is validated once |
| final rc | the newest rc's `rc-validation` status is `success` and no backport is pending | nothing, unless it is a `release-blocker` worth another rc |
| released | `vX.Y.Z` exists | branch is read-only; the backport label is gone |

## Roles

**Developer** opens PRs against `main` with a Conventional Commit title, as
always. To get a fix into a release that is being validated, add the label
`backport release-X.Y.Z` (before or after merge) for every open release
branch that needs it. On a conflict, resolve it in the draft PR the bot opened
(below). Never open a PR against a release branch except for a labelled
`release-only` change.

**Release manager** (team `npu-release-managers`) decides when to cut and of
which kind, which backports go in (see "What a backport may carry"), and when
to tag GA. They are the only people who can merge into `release-*` or create
`v*` tags, and the only ones who can run `cut-release` / `tag-release`.

**Automation** (GitHub Actions, a GitHub App identity, two rulesets) does the
rest: cherry-picks, policy checks, image/chart/OLM publishing, notifications.

## One-time GitHub setup

Do these once, in this order. Everything here is idempotent.

1. **Team.** Create the org team `npu-release-managers` with 2–3 members.
   (Change `vars.RELEASE_TEAM` if you pick another slug.)

2. **GitHub App** (org owner, ~10 minutes). Org settings → Developer settings →
   GitHub Apps → New GitHub App.
   - Name `rbln-release-bot`, any homepage URL, **uncheck Webhook**.
   - Repository permissions: Contents *read & write*, Pull requests *read &
     write*, Issues *read & write* (labels), Commit statuses *read*
     (`tag-release` reads the `rc-validation` status before GA), Metadata
     *read*.
   - Organization permissions: Members *read* (the workflows check the actor's
     team membership).
   - "Only on this account". Create, note the **App ID**, generate a private
     key, then **Install App** on this repository.
   - This identity is required, not optional: it is what bypasses the
     rulesets, and PRs it opens trigger CI (PRs opened with `GITHUB_TOKEN` do
     not, which would leave backport PRs unmergeable).

3. **Secrets and variables** (Settings → Secrets and variables → Actions).

   | Kind | Name | Value |
   |---|---|---|
   | variable | `RELEASE_APP_ID` | the App ID |
   | secret | `RELEASE_APP_PRIVATE_KEY` | contents of the App's `.pem` |
   | variable | `RELEASE_TEAM` | `npu-release-managers` (optional, this is the default) |
   | secret | `BUILDKITE_API_TOKEN` | API token with `read_builds` + `write_builds` for the test-infra matrix pipeline (on obedients, the same token other repos keep as `OBEDIENTS_API_TOKEN`; prefer a bot account) |
   | variable | `BUILDKITE_API_URL` | REST base URL. The matrix runs on obedients, the in-house Buildkite-compatible CI: `https://obedients-api.k8s.rebellions.in/v2`. Default is `https://api.buildkite.com/v2` |
   | variable | `BUILDKITE_ORG`, `BUILDKITE_VALIDATE_PIPELINE` | organization slug (obedients: `main`) and the slug of the `npu-operator-test-infra` matrix pipeline (the validation trigger is skipped with a notice when unset) |
   | variable | `BUILDKITE_VALIDATE_BRANCH` | test-infra branch that holds the pipeline (optional, default `dev`) |
   | variable | `RC_VALIDATION_SENTINEL_REF` | optional sentinel ref to pin for rc validations |
   | variable | `RC_VALIDATION_ENFORCE` | `true` to refuse GA when the newest rc has no `success` `rc-validation` status (warn-only until then, so the gate can ship before the pipeline reports) |
   | variable | `PREFLIGHT_ENFORCE` | `true` to make the rc preflight check blocking (warn-only until then) |

   Already present and reused: `DOCKERHUB_*`, `HARBOR_*`, `CERTIFIED_OPERATORS_*`,
   `SLACK_OAUTH_TOKEN`, `SLACK_CHANNEL_ID`.

4. **Labels.**

   ```bash
   hack/release/setup-labels.sh
   ```

5. **Rulesets.** Definitions are in `.github/rulesets/`; apply them with the
   team slug and App ID (needs repo admin):

   ```bash
   hack/release/apply-rulesets.sh --team npu-release-managers --app-id <APP_ID>
   ```

   `release-branches` (all `release-*`): PR required, one approval from the
   team, squash only, required checks `[OP] PR CI`, `release-policy` and the
   GitHub Actions PR jobs (title, code-check, unit tests, build, helm,
   bundle), linear history, no force-push/delete, and **only the team (via
   PR) or the App may update or create** these branches. `release-tags` (all
   `v*`): only the team or the App may create, move or delete tags.
   `protect-main` (default branch): the same required checks; without it only
   the Buildkite status gated merges and a red unit-test run could be merged.

6. **Repository settings.** Settings → General → Pull Requests: enable
   **Allow auto-merge** (the backport bot relies on it). Keep squash as the
   only merge method.

7. **Buildkite (obedients).**
   - Add `release-*` to the PR pipeline's branch filter so backport PRs get
     `[OP] PR CI`.
   - The rc validation creates a build of the `npu-operator-test-infra`
     matrix pipeline through the Buildkite REST Builds API (`POST
     .../pipelines/<slug>/builds` with `commit`, `branch`, `message`, `env`),
     which obedients serves at `BUILDKITE_API_URL`. No pipeline trigger
     (webhook) is needed; a trigger's values are fixed when it is created and
     cannot carry the per-rc chart version and commit.
   - That pipeline must accept the build env `hack/release/validate-rc.sh`
     sends (`RELEASE_MODE`, `CLUSTERS`, `HELM_CHART`, `HELM_CHART_VERSION`,
     `SENTINEL_CATALOG_IMAGE`, `RC_STATUS_*`) and post the verdict as the
     `rc-validation` commit status on this repository; its agent's `GIT_PAT`
     therefore needs `repo:status` here. Superseded validations are cancelled
     by the script (best effort, by build message), not by a pipeline setting;
     keep "Skip/Cancel Intermediate Builds" **off** on that pipeline, since
     the matrix nightly shares its `dev` branch.
   - A validation and the matrix nightly can overlap; check the OpenStack
     quota allows two sets of dynamic clusters, or keep the nightly window
     clear of rc tags.

8. **Security.** Settings → Code security → enable **Private vulnerability
   reporting**. `SECURITY.md` states the support policy.

## Workflows and scripts

| Workflow | Trigger | Does |
|---|---|---|
| `cut-release` | manual (`kind` = minor / patch / major, optional `version` override) | computes the version, creates `release-X.Y.Z` from `main` (minor, major) or from the newest GA tag (patch), tags `vX.Y.Z-rc1`, creates label `backport release-X.Y.Z`, prints the commit tally, Slack |
| `tag-release` | manual (`branch`, `kind`) or a PR labelled `tag-rc` merged into `release-*` | tags the branch HEAD as the next rc or as GA (guards: HEAD already tagged, version already GA, GA ≠ newest rc, GA without a `success` `rc-validation` status) |
| `backport` | main PR merged, or labelled after merge | cherry-picks with `-x`, opens the backport PR with auto-merge, drafts on conflict, labels drafts `backport-manual`, Slack |
| `release-policy` | PR into `release-*` (open, push, label change) | required check: title type, no merges, cherry-pick trailer, origin on main, patch-id equality, `release-only` rationale, size warning |
| `release.yaml` | `v*` tag push | rc path or GA path (see below) |

Every workflow is a thin wrapper around a script in `hack/release/`, so the
same steps can be run by hand (for a dry run, add yourself to the ruleset
bypass temporarily):

```bash
hack/release/cut-release.sh minor            # release-X.(Y+1).0 from main + rc1 + label
hack/release/cut-release.sh patch            # release-X.Y.(Z+1) from the newest GA tag
hack/release/tag-release.sh release-0.7.0 rc # next rc
hack/release/tag-release.sh release-0.7.0 ga # GA (newest rc's commit, rc-validation success)
hack/release/validate-rc.sh v0.7.0-rc2       # re-trigger the matrix validation of an rc
hack/release/olm-graph-check.sh 0.7.0        # upgrade-graph check against upstream
```

## What `release.yaml` publishes

| | rc (`vX.Y.Z-rcN`) | GA (`vX.Y.Z`) |
|---|---|---|
| pre-check | tag is on `release-X.Y.Z` | tag commit == newest rc commit **and** its `rc-validation` status is `success`, else fail before publishing |
| images | built (amd64 + arm64) → `harbor.k8s.rebellions.in/rebellions/...:vX.Y.Z-rcN` | **not built**: rc digests copied to `docker.io/rebellions/...:vX.Y.Z` and `:latest` |
| chart | `oci://harbor.k8s.rebellions.in/rebellions`, version `X.Y.Z-rcN`, operator/validator pointed at Harbor | `oci://docker.io/rebellions`, version `X.Y.Z`, re-packaged from the same commit |
| component digests | every image `values.yaml` pins, resolved once and attached to the pre-release as `component-digests.env` | reused as-is for the bundle; the tags are not resolved again |
| OLM | bundle validate (+ optional suites), single-bundle FBC validate, **upgrade-graph check** against upstream templates, catalog image → Harbor (`rbln-npu-operator-catalog:vX.Y.Z-rcN`) for the OLM install scenario | graph check again, bundle with the rc's digests, PR to certified-operators (`merge: true`) |
| certification | `preflight check container` on both images (warn-only until `PREFLIGHT_ENFORCE=true`) | – |
| GitHub | pre-release with the chart and the digests file attached | draft release with notes and the chart; release manager publishes |
| validation | one `npu-operator-test-infra` matrix build in release mode (all dynamic clusters + sandbox); its verdict lands as the `rc-validation` commit status on the rc commit; a newer rc cancels the older rc's build | – |
| labels | – | `backport release-X.Y.Z` deleted |
| Slack | summary | summary |

Release notes cover `previous GA..this tag`. A backport whose `main` origin is
in the range is dropped because the origin is listed; on a patch branch the
origin is not in the range, so the backport itself is listed. `main` commits
that already shipped through the previous release's backports are dropped
too. A component-image table from `values.yaml` is appended.

## Day to day

### Cut

Before a minor: `main`'s last nightly is green; Renovate/component bump PRs
that should ride this release are merged (whatever is pinned in `values.yaml`
at the cut is the combination the rc validates and GA ships). Then:

```bash
gh workflow run cut-release.yaml -f kind=minor   # release-X.(Y+1).0 from main HEAD
```

or Actions → `cut-release` → Run workflow. The number is computed from the
newest GA tag; `version` is only for the rare override. Within minutes:
branch, `rc1`, label, Harbor images, chart and catalog, GitHub pre-release,
the validation build, and a Slack post telling developers the label name.

### Validating

- Developers put `backport release-0.7.0` on the `main` PR that carries the fix.
- The bot opens the backport PR; `[OP] PR CI` and `release-policy` run on it.
- A release manager reviews against "What a backport may carry" and approves.
  Approval is the merge: auto-merge fires once checks are green.
- To publish a new rc right away, put `tag-rc` on the backport PR before
  approving (or on the `main` PR; it is copied). **Batch backports and label
  only the last one**: every rc runs the whole matrix (6–9 hours, three
  dynamic clusters), and a new rc cancels the validation of the previous one.
  Or run `tag-release` by hand:

  ```bash
  gh workflow run tag-release.yaml -f branch=release-0.7.0 -f kind=rc
  ```

- Every rc runs the OLM pre-validation and preflight; a failure there is
  fixed on `main` and backported like any other fix.
- The validation verdict arrives as the `rc-validation` status on the rc
  commit (release branch → commits → the check mark; the link opens the
  Buildkite build). `error` means the validation infrastructure failed
  (preflight, cluster provisioning): rebuild the build. `failure` means the
  rc is broken: fix on `main`, backport, new rc.

### What a backport may carry

In: security fixes, regressions against the previous GA, bugs that lose
data, crash or hang a component, upgrade or install failures, and the
documentation/CI/test changes needed to ship them. Out: features, refactors,
dependency bumps whose only purpose is to silence a scanner, and fixes for
issues that only occur with a default-off option. `release-policy` enforces
the mechanics (title type, provenance); this list is what the release
manager's approval means.

### Conflicts

The bot commits the conflicted state as a **draft PR** (commit message
`BACKPORT-CONFLICT`, no trailer), labels it `backport-manual`, and comments
the recovery steps. The developer redoes the cherry-pick on the bot's branch;
do not amend the conflict commit, it has no trailer:

```bash
git fetch origin backport-123-to-release-0.7.0
git switch backport-123-to-release-0.7.0
git reset --hard HEAD^                 # drop BACKPORT-CONFLICT
git cherry-pick -x <main squash sha>   # same conflict, resolve, git add
git cherry-pick --continue             # keeps the original message + trailer
git push --force-with-lease
gh pr ready
```

`release-policy` then passes checks 3–4 (trailer, origin on main) and accepts
the content difference because of `backport-manual`. Drafts cannot carry
auto-merge, so a release manager merges after approving.

### Final rc and GA

The final rc is not announced, it is observed: the newest rc's
`rc-validation` status is `success` and no backport is pending. Because a
validation takes most of a day, **the last backport should merge two days
before the GA date**. From then on nothing merges unless it is a
`release-blocker` worth another rc and another validation; there is no switch
to flip, because only release managers can merge. Then:

```bash
gh workflow run tag-release.yaml -f branch=release-0.7.0 -f kind=ga
```

`tag-release` and then `release.yaml` verify both invariants (newest rc,
`rc-validation` success), promote, publish, open the certified-operators PR
and draft the notes. The release manager edits and publishes the draft and
watches the certified-operators PR (`/re-trigger` if its pipeline fails). The
backport label is deleted; the branch is done.

### After GA

Bugs go to `main`. If one is a blocker for users of the current GA, cut a
patch from that GA's tag:

```bash
gh workflow run cut-release.yaml -f kind=patch   # release-0.7.1 from v0.7.0
```

The branch starts as the GA plus nothing; the fixes arrive as labelled
backports from `main`, exactly like during a minor, and the same validation
and GA steps apply. `0.7.1` therefore carries fixes only. A patch is refused
when its GA is no longer the newest one: publish the fix in the next minor
instead. If a minor is being validated at the same time, label the fix for
both branches and tag the patch GA before the minor GA, so the OLM chain
reads `0.7.1 replaces 0.7.0`, `0.8.0 replaces 0.7.1`.

## Labels

| Label | Where | Meaning |
|---|---|---|
| `backport release-X.Y.Z` | `main` PR | cherry-pick this fix into that release branch. Created at cut, deleted at GA |
| `backport-manual` | backport PR | conflict resolved by hand; content may differ from the `main` commit |
| `release-only` | PR into `release-*` | no counterpart on `main`; body must contain a "Why not main" section; release manager approval required |
| `release-blocker` | issue / PR | worth another rc and another validation after the final rc; priority marker only |
| `tag-rc` | backport PR (or `main` PR, copied) | tag the next rc as soon as this merges |

## `release-policy` checks

| # | Check | Fails when | Effect |
|---|---|---|---|
| 1 | title type | not one of `fix revert docs test ci build chore`, or a `!` marker | block |
| 2 | no merge commits | a merge commit is in the PR | block |
| 3 | cherry-pick trailer | a commit lacks `(cherry picked from commit <sha>)` | block (skipped with `release-only`) |
| 4 | origin on main | the sha is unknown or not an ancestor of `origin/main` | block (skipped with `release-only`) |
| 5 | content equals origin | `git patch-id` differs from the origin commit | block unless `backport-manual` |
| 6 | `release-only` rationale | no "Why not main" section in the body | block |
| 7 | size | > 400 changed lines outside generated paths | comment only |

## Troubleshooting

- **GA workflow fails in `classify`.** Either the GA tag is not on the newest
  rc's commit (something merged after the last rc), or that commit's
  `rc-validation` status is not `success`. Nothing was published. For the
  first, tag a new rc and let it validate. For the second, read the status:
  `error` is the validation infrastructure (rebuild the Buildkite build, or
  `hack/release/validate-rc.sh vX.Y.Z-rcN`), `failure` is the rc (fix on
  `main`, backport, new rc), `none` means the validation never ran (check the
  `BUILDKITE_*` variables and the test-infra pipeline). Until
  `RC_VALIDATION_ENFORCE` is `true` this is a warning, not a failure.
- **`cut-release` refuses a patch.** Its parent is not the newest GA. Only
  the newest GA can be patched; ship the fix in the next minor.
- **Backport PR has no checks / cannot merge.** The PR was created with
  `GITHUB_TOKEN` instead of the App token, or the App is missing from the
  ruleset bypass list. Check `RELEASE_APP_ID` / `RELEASE_APP_PRIVATE_KEY` and
  re-run `apply-rulesets.sh`.
- **"blocked by ruleset" on an approved backport PR.** Auto-merge merges as
  the App; the App must be a bypass actor of `release-branches`.
- **`cut-release` fails with "not an active member".** The actor is not in
  `npu-release-managers`, or the App lacks the org permission Members: read.
- **Upgrade-graph check fails at GA but passed at rc.** Another version was
  merged into certified-operators in between. Re-run after checking the
  templates; the check tells you which channel has two heads.
- **No Slack messages.** `SLACK_OAUTH_TOKEN` or the channel variable is
  missing; the workflows log a notice and continue.
