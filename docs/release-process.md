# Release process

How a version of rbln-npu-operator goes from `main` to docker.io, the Helm OCI
registry and the Red Hat certified-operators catalog, who does what along the
way, and how the repository has to be configured for it. The design record
behind these choices lives in `docs/superpowers/specs/` (local only).

## Principles

1. **`main` is always open and always releasable.** There is no code freeze.
   Unfinished work stays on a feature branch or behind a default-off flag.
2. **A release starts when a release manager cuts `release-X.Y.Z` from `main`.**
   The branch name is the version. Which position bumps (major, minor, patch)
   is the release manager's decision; the tooling only prints a suggestion.
3. **Only fixes that already live on `main` enter a release branch**, via a
   label that makes a bot cherry-pick them. Only the release-manager team can
   merge there.
4. **Tags drive everything.** `vX.Y.Z-rcN` publishes a release candidate to
   the internal registry and starts the soak. `vX.Y.Z` promotes the newest rc
   by digest to docker.io, publishes the chart and opens the OLM bundle PR.
   GA is refused unless it is the exact commit of the newest rc.
5. **The tooling knows no dates.** "Monthly, last Friday" is a team habit the
   release manager keeps in their head; the process reads its state from
   branches, tags and labels.
6. **Only the latest GA is supported.** Bugs are fixed on `main` and shipped
   as the next cut. A hotfix is just another cut (`release-X.Y.Z+1`) from
   `main`, soaked briefly.

## States

| State | How you tell | What is allowed |
|---|---|---|
| open | no unreleased `release-*` branch | anything on `main` |
| soaking | `release-X.Y.Z` has rc tags but no GA tag | labelled fix backports, each producing a new rc |
| final rc | release manager announced "last rc" | nothing, unless it is a `release-blocker` worth delaying GA for |
| released | `vX.Y.Z` exists | branch is read-only; the backport label is gone |

## Roles

**Developer** opens PRs against `main` with a Conventional Commit title, as
always. To get a fix into the rc that is soaking, add the label
`backport release-X.Y.Z` (before or after merge). On a conflict, resolve it in
the draft PR the bot opened (below). Never open a PR against a release branch
except for a labelled `release-only` change.

**Release manager** (team `npu-release-managers`) decides when to cut, which
backports go in, when the last rc is, and when to tag GA. They are the only
people who can merge into `release-*` or create `v*` tags, and the only ones
who can run `cut-release` / `tag-release`.

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
     write*, Issues *read & write* (labels), Metadata *read*.
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
   | secret | `BUILDKITE_API_TOKEN` | token with `write_builds` for the soak pipeline |
   | variable | `BUILDKITE_ORG`, `BUILDKITE_SOAK_PIPELINE` | Buildkite org and pipeline slugs (soak trigger is skipped with a notice when unset) |
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
   team, squash only, required checks `[OP] PR CI` and `release-policy`,
   linear history, no force-push/delete, and **only the team (via PR) or the
   App may update or create** these branches. `release-tags` (all `v*`): only
   the team or the App may create, move or delete tags.

6. **Repository settings.** Settings → General → Pull Requests: enable
   **Allow auto-merge** (the backport bot relies on it). Keep squash as the
   only merge method.

7. **Buildkite.** Add `release-*` to the PR pipeline's branch filter so
   backport PRs get `[OP] PR CI`, and make sure the nightly pipeline accepts
   `HELM_CHART` / `HELM_CHART_VERSION` from the API (it already does).

8. **Security.** Settings → Code security → enable **Private vulnerability
   reporting**. `SECURITY.md` states the support policy.

## Workflows and scripts

| Workflow | Trigger | Does |
|---|---|---|
| `cut-release` | manual (`version`, `sha`) | creates `release-X.Y.Z`, tags `vX.Y.Z-rc1`, creates label `backport release-X.Y.Z`, prints the commit tally, Slack |
| `tag-release` | manual (`branch`, `kind`) or a PR labelled `tag-rc` merged into `release-*` | tags the branch HEAD as the next rc or as GA (guards: HEAD already tagged, version already GA, GA ≠ newest rc) |
| `backport` | main PR merged, or labelled after merge | cherry-picks with `-x`, opens the backport PR with auto-merge, drafts on conflict, labels drafts `backport-manual`, Slack |
| `release-policy` | PR into `release-*` (open, push, label change) | required check: title type, no merges, cherry-pick trailer, origin on main, patch-id equality, `release-only` rationale, size warning |
| `release.yaml` | `v*` tag push | rc path or GA path (see below) |

Every workflow is a thin wrapper around a script in `hack/release/`, so the
same steps can be run by hand (for a dry run, add yourself to the ruleset
bypass temporarily):

```bash
hack/release/cut-release.sh 0.7.0            # branch + rc1 + label
hack/release/tag-release.sh release-0.7.0 rc # next rc
hack/release/tag-release.sh release-0.7.0 ga # GA (must be the newest rc's commit)
hack/release/olm-graph-check.sh 0.7.0        # upgrade-graph check against upstream
```

## What `release.yaml` publishes

| | rc (`vX.Y.Z-rcN`) | GA (`vX.Y.Z`) |
|---|---|---|
| pre-check | tag is on `release-X.Y.Z` | tag commit == newest rc commit, else fail before publishing |
| images | built (amd64 + arm64) → `harbor.k8s.rebellions.in/rebellions/...:vX.Y.Z-rcN` | **not built**: rc digests copied to `docker.io/rebellions/...:vX.Y.Z` and `:latest` |
| chart | `oci://harbor.k8s.rebellions.in/rebellions`, version `X.Y.Z-rcN`, operator/validator pointed at Harbor | `oci://docker.io/rebellions`, version `X.Y.Z`, re-packaged from the same commit |
| OLM | bundle validate (+ optional suites), single-bundle FBC validate, **upgrade-graph check** against upstream templates | graph check again, bundle with digests, PR to certified-operators (`merge: true`) |
| certification | `preflight check container` on both images (warn-only until `PREFLIGHT_ENFORCE=true`) | – |
| GitHub | pre-release with the chart attached | draft release with notes and the chart; release manager publishes |
| soak | Buildkite build with `HELM_CHART=oci://…/rbln-npu-operator-chart`, `HELM_CHART_VERSION=X.Y.Z-rcN` | – |
| labels | – | `backport release-X.Y.Z` deleted |
| Slack | summary | summary |

Release notes cover `previous GA..this tag`. Backport commits (those with a
cherry-pick trailer) are dropped because their `main` origin is listed, and
`main` commits that already shipped through the previous release's backports
are dropped too. A component-image table from `values.yaml` is appended.

## Day to day

### Cut

Before: `main`'s last nightly is green; Renovate/component bump PRs that
should ride this release are merged (whatever is pinned in `values.yaml` at
the cut is the "verified combination"). Then:

```bash
gh workflow run cut-release.yaml -f version=0.7.0     # sha defaults to main HEAD
```

or Actions → `cut-release` → Run workflow. Within minutes: branch, `rc1`,
label, Harbor images and chart, GitHub pre-release, soak build, Slack post
telling developers the label name. Pick the number before pressing: the
branch cannot be renamed, and deleting one needs a ruleset bypass.

### Soaking

- Developers put `backport release-0.7.0` on the `main` PR that carries the fix.
- The bot opens the backport PR; `[OP] PR CI` and `release-policy` run on it.
- A release manager reviews (is it a fix? does the rc need it?) and approves.
  Approval is the merge: auto-merge fires once checks are green.
- To publish a new rc right away, put `tag-rc` on the backport PR before
  approving (or on the `main` PR; it is copied). To batch several backports
  into one rc, label only the last one, or run `tag-release` by hand:

  ```bash
  gh workflow run tag-release.yaml -f branch=release-0.7.0 -f kind=rc
  ```

- Every rc runs the OLM pre-validation and preflight; a failure there is
  fixed on `main` and backported like any other fix.

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

When soak is quiet and the target date is near, the release manager announces
"last rc" in the channel. From then on nothing merges unless it is a
`release-blocker` worth delaying GA for; there is no switch to flip, because
only release managers can merge. After the last rc has had enough nightly
runs (three is the habit):

```bash
gh workflow run tag-release.yaml -f branch=release-0.7.0 -f kind=ga
```

`release.yaml` verifies the invariant, promotes, publishes, opens the
certified-operators PR and drafts the notes. The release manager edits and
publishes the draft and watches the certified-operators PR (`/re-trigger` if
its pipeline fails). The backport label is deleted; the branch is done.

### After GA

Bugs go to `main`. If one is a blocker for users of the current GA, cut a
hotfix from `main`:

```bash
gh workflow run cut-release.yaml -f version=0.7.1
```

Same soak (two nightlies is the habit), same GA step. `0.7.1` contains
everything merged to `main` since the `0.7.0` cut; the number says "mostly
fixes, safe to take", not "fixes only".

## Labels

| Label | Where | Meaning |
|---|---|---|
| `backport release-X.Y.Z` | `main` PR | cherry-pick this fix into that release branch. Created at cut, deleted at GA |
| `backport-manual` | backport PR | conflict resolved by hand; content may differ from the `main` commit |
| `release-only` | PR into `release-*` | no counterpart on `main`; body must contain a "Why not main" section; release manager approval required |
| `release-blocker` | issue / PR | candidate for the rc even after "last rc"; priority marker only |
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

- **GA workflow fails in `classify`.** The GA tag is not on the newest rc's
  commit (something merged after the last rc). Tag a new rc, soak, tag GA
  again. Nothing was published.
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
