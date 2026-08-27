# Rolling node-by-node migration to family-scoped driver DaemonSets

## When to use this guide

Use this guide when upgrading the operator from a **pre-family release
(chart ≤ 0.4.5)** to a **family-scoped release (chart ≥ 0.4.6)** on a
cluster whose NPU nodes must be migrated one at a time.

Check whether it applies to you:

```bash
export NS=<operator-namespace>          # helm release namespace
kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  -L rebellions.ai/driver-node-pool
```

If the pool names have **no family prefix** (`ubuntu22.04-5.15.0-…` rather
than `atom-ubuntu22.04-5.15.0-…`), you are on a pre-family release and this
guide applies. A plain `helm upgrade` would replace the driver on **every
node at once**, taking NPU capacity down cluster-wide.

With this procedure, each node keeps running the old driver until you
release it with a single label flip. Impact per released node: its NPU
workloads are evicted and the driver is reinstalled (one to a few minutes).
Held nodes are not disturbed.

Terms used below:

- **old CR** — a pre-upgrade `RBLNDriver` (the default install has one,
  named `rbln-driver`).
- **family CR** — a `RBLNDriver` rendered from the new chart's
  `driver.instances` (e.g. `rbln-driver-atom`).
- **held node** — still runs the old driver; **released node** — flipped to
  the new one.

The main steps assume the default single-CR install. If you created more
`RBLNDriver` CRs, also read the [appendix](#appendix--multiple-old-crs).

## Procedure at a glance

1. Label every NPU node `rbln-mig/stage=old` and point the old CR's
   `nodeSelector` at it (Step 1).
2. Pin the old CR to a nonexistent image — the operator now keeps the old
   driver in place (Step 2).
3. Upgrade the operator; nothing changes on any node yet (Step 3).
4. Flip one node's label to `stage=new` — that node alone gets the new
   driver. Repeat per node (Step 4).
5. Delete the old CR, drop the stage selectors, remove the labels (Step 5).

## Prerequisites

1. **NFD is installed and healthy.** The new operator labels nodes with
   `rebellions.ai/npu.family` through NFD; without it no family CR can run:

   ```bash
   kubectl get crd nodefeaturerules.nfd.k8s-sigs.io
   ```

2. **Family-scoped driver images are published** for every
   (family, os, kernel) combination in your fleet. Check with any registry
   client (e.g. [`crane`](https://github.com/google/go-containerregistry)):

   ```bash
   # <registry>/<repo-prefix>/<family>/rbln-driver:<version>-<kernel>-<os>
   crane manifest repo.rebellions.ai/rebellions/atom/rbln-driver:3.2.2-5.15.0-100-generic-ubuntu22.04 > /dev/null && echo OK
   ```

   A released node will not receive a new driver until its image exists
   (the family CR reports `DriverImageNotFound` with the exact missing
   paths). Verify first, and release a single canary node before the rest.

3. **`operator.driverImageCheck` stays enabled** (the default) — step 2
   depends on it.

4. **Choose a hold image path that returns 404**: a nonexistent repository
   in the same registry the CR's `imagePullSecrets` authenticate to
   (e.g. `rebellions/rbln-driver-hold`). A registry that answers 401/403
   instead of 404 does **not** hold anything, and the old DaemonSet is
   deleted immediately.

5. **Download the new chart** (its CRDs are needed in step 2):

   ```bash
   helm pull <new-chart-ref> --version <new-version> --untar --untardir /tmp
   # → /tmp/rbln-npu-operator-chart/crds/
   ```

6. **All driver pods are `Running` and `Ready`:**

   ```bash
   kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
   ```

## Step 1 — Label the nodes `stage=old`

Label the nodes **first**, then add the selector to the old CR — the
reverse order deselects unlabeled nodes and deletes their driver pods:

```bash
kubectl label nodes -l rebellions.ai/npu.deploy.driver=true rbln-mig/stage=old
```

Then set the selector through a values-only upgrade of the **old** chart
version:

```yaml
driver:
  nodeSelector:
    rbln-mig/stage: old
```

```bash
helm upgrade rbln-npu-operator <old-chart-ref> --version <old-version> \
  -n "$NS" -f <old-values.yaml>
```

Running driver pods are not restarted (the DaemonSet updates on delete
only). Success check: driver pods keep their age, and the DaemonSet's
`NODE SELECTOR` column now includes `rbln-mig/stage=old`:

```bash
kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver
```

## Step 2 — Keep the old DaemonSets in place

### 2.1 Protect the old CR from helm

The new values set `deployDefault: false`, which drops the default CR from
the release. This annotation makes helm leave the live object alone:

```bash
kubectl annotate rblndriver rbln-driver helm.sh/resource-policy=keep
```

### 2.2 Apply the new RBLNDriver CRD

Needed so the `spec.smd` patch below is accepted (the old CRD silently
drops the field). `--force-conflicts` because the CRD is also
field-managed by helm and by the chart's `crd-apply` hook:

```bash
kubectl apply --server-side --force-conflicts \
  -f /tmp/rbln-npu-operator-chart/crds/rebellions.ai_rblndrivers.yaml
```

Apply **only** this CRD — applying the RBLNClusterPolicy CRD early starts
the metrics gap before the upgrade instead of at it.

### 2.3 Pin the old CR's driver image to the hold path

This is what keeps the old DaemonSet alive through the upgrade
(prerequisite 4). Running pods are not affected:

```bash
kubectl patch rblndriver rbln-driver --type=merge \
  -p '{"spec":{"image":"<repo-prefix>/rbln-driver-hold"}}'
```

### 2.4 Set the old CR's smd image

Old CRs predate `spec.smd`; without this the CRD default applies, and if
that path is absent from your registry, metrics stay down on every held
node:

```bash
kubectl patch rblndriver rbln-driver --type=merge \
  -p '{"spec":{"smd":{"registry":"<registry>","image":"<repo-prefix>/rbln-smd"}}}'
```

Confirm both patches were applied before moving on:

```bash
kubectl get rblndriver rbln-driver -o jsonpath='{.spec.image}{"  "}{.spec.smd}{"\n"}'
```

## Step 3 — Prepare the new values, then upgrade

Write the new values **before** upgrading — running the upgrade with your
old values file would re-render the default CR and overwrite the image pin
from step 2.3, releasing every node at once:

```yaml
# new-values.yaml — driver block shown; keep the rest of your values as-is
driver:
  enabled: true
  deployDefault: false          # the family CRs replace the default CR
  instances:
    atom:                       # rendered name: rbln-driver-atom
      image:
        tag: "3.2.2"
      nodeSelector:
        rebellions.ai/npu.family: atom
        rbln-mig/stage: "new"
```

One `instances` entry per NPU family in your fleet, each selecting its
family label **and** `rbln-mig/stage: "new"`. Old-CR and family-CR
selectors must never match the same node: a node matching both leaves the
old CR owning no nodes, and all its DaemonSets are deleted at once. The
stage label guarantees this cannot happen.

`new-values.yaml` must be your **complete** values file (registry, image
pull secrets, operand settings, …) with the `driver` block updated as
above — `helm upgrade` does not reuse the previous release's values, so
passing this fragment alone would reset every other setting to the chart
defaults:

```bash
helm upgrade rbln-npu-operator <new-chart-ref> --version <new-version> \
  -n "$NS" -f new-values.yaml
```

Verify the held state before releasing any node:

```bash
kubectl get rblndriver
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
kubectl get nodes -L rebellions.ai/npu.family,rbln-mig/stage
```

- The old CR reports `Ready=False`, `reason: DriverImageNotFound`, naming
  the hold path. **This is the hold working, not a fault.**
- Old driver pods are still `Running` on every node (`UP-TO-DATE 0` on
  their DaemonSet is normal) and NPU resources are still advertised.
- Family CRs are `ready` with `DESIRED 0` (they own no nodes yet).
- **Every NPU node shows the expected `rebellions.ai/npu.family` value**
  (applied by NFD within about a minute of the upgrade). Do not release a
  node whose family label is missing — it would lose its old driver and
  match no family CR.
- The legacy `rbln-daemon` DaemonSet is replaced by per-CR `<cr>-smd`
  DaemonSets; expect one brief metrics-exporter restart per node. If an
  smd pod is stuck on a wrong image (step 2.4 ran late), delete the pod.

**Do not reboot held nodes** — a rebooted node would reinstall through the
hold image and fail. Release it instead.

## Step 4 — Release nodes one at a time

Start with a canary. Confirm its family label, then flip its stage label:

```bash
NODE=<canary-node>
kubectl get node "$NODE" -L rebellions.ai/npu.family   # must show the family
kubectl label node "$NODE" rbln-mig/stage=new --overwrite
```

On this node only: the old driver pod terminates, the node moves to its
family CR, and the new driver pod evicts the node's NPU pods, unloads the
kernel module and installs the family-scoped driver.

Success check before the next node:

```bash
# the node's driver pod is 1/1 Ready
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  --field-selector spec.nodeName="$NODE"

# NPU resources are advertised again
kubectl get node "$NODE" -o jsonpath='{.status.allocatable}' | tr ',' '\n' | grep rebellions.ai
```

Release the remaining nodes at your own pace; flip several labels at once
to migrate a batch. When the old CR loses its last node, the operator
removes its now-empty DaemonSets automatically.

## Step 5 — Finish

1. Delete the old CR (all nodes carry `stage=new`; the old CR shows
   `DESIRED 0`):

   ```bash
   kubectl delete rblndriver rbln-driver
   ```

2. Drop the stage key from the family CR selectors
   (`nodeSelector: {rebellions.ai/npu.family: <family>}`) with a
   values-only upgrade. Only **then** remove the node labels — removing a
   label a family CR still selects deletes that node's driver pod:

   ```bash
   # final-values.yaml = new-values.yaml with only the stage keys removed
   helm upgrade rbln-npu-operator <new-chart-ref> --version <new-version> \
     -n "$NS" -f final-values.yaml
   kubectl label nodes -l rbln-mig/stage rbln-mig/stage-
   ```

3. Final check — every family CR `Ready`, no flat DaemonSets left:

   ```bash
   kubectl get rblndriver
   kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver \
     -L rebellions.ai/driver-node-pool
   ```

## If something goes wrong

- **A released node's new driver pod fails:** stop releasing further
  nodes — every held node keeps running the old driver untouched. Resolve
  the failure on that node, or abort (below). Flipping the node's label
  back while the new operator is still running does **not** restore the
  old driver: the held DaemonSet's template points at the hold image.
- **A family image is missing:** nothing breaks until a node is released;
  the family CR names the missing paths (`DriverImageNotFound`). Publish
  them before releasing more nodes; a node released too early sits without
  a driver until the image appears (the check re-runs within ~5 minutes).

### Aborting the migration

Run these in order — the order matters. Step 1 relies on helm adopting the
kept old CR without touching its spec; this was validated with helm v3.17.

```bash
# 1. Downgrade the operator by upgrading BACK to the old chart and your
#    complete old values file.
#    Do not use `helm rollback`: the kept old CR is absent from the current
#    release manifest, and rollback aborts halfway with
#    `no RBLNDriver with the name "rbln-driver" found`.
#    This step also deletes the family CRs, so released nodes lose their
#    new driver pod here.
helm upgrade rbln-npu-operator <old-chart-ref> --version <old-version> \
  -n "$NS" -f <old-values.yaml>

#    Confirm the downgrade left the held nodes intact before continuing:
#    old driver pods still Running, hold image still pinned.
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
kubectl get rblndriver rbln-driver -o jsonpath='{.spec.image}{"\n"}'

# 2. Un-pin the old CR — only AFTER the downgrade. Un-pinning while the
#    new operator is still running releases the hold, and the old
#    DaemonSet is replaced on every held node at once.
kubectl patch rblndriver rbln-driver --type=merge \
  -p '{"spec":{"image":"<original-repo>/rbln-driver"}}'

# 3. Bring released nodes back, one at a time. Each flip reinstalls the
#    old flat driver on that node:
kubectl label node <released-node> rbln-mig/stage=old --overwrite

#    The old operator does not watch node labels. If no driver pod appears
#    on the node within a minute, trigger reconciliation with a spec change:
kubectl patch rblndriver rbln-driver --type=merge \
  -p '{"spec":{"annotations":{"rbln-mig/kick":"1"}}}'

# 4. Verify: the old CR is Ready and a driver pod is Running on every node.
kubectl get rblndriver rbln-driver
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
```

Held nodes remain unaffected — their old pods never stopped.

## After this migration

This procedure is only needed for the one-time flat → family transition.
Later version bumps keep the DaemonSet identity and are paced per node by
the upgrade controller (`upgradePolicy.maxParallelUpgrades`), and the same
label-based method can move nodes between `RBLNDriver` CRs for canary
rollouts.

## Appendix — multiple old CRs

If the pre-upgrade cluster runs more than one `RBLNDriver` (e.g. one per
NPU family, created manually because pre-0.4.6 charts render only one):

- **Distinct stage values.** Give each old CR its own value (`old-atom`,
  `old-r100`, …) in step 1, so their selectors stay disjoint from each
  other as well. `rebellions.ai/npu.driver.owner` and
  `rebellions.ai/npu.deploy.driver` are reserved and rejected as selector
  keys.
- **Repeat step 2.3/2.4 per CR.** Only helm-owned CRs need the step 2.1
  annotation; manually-created CRs need nothing — but do **not** add helm
  adoption metadata to them: helm adoption does not converge an adopted
  object's spec, and a name collision with a rendered family CR fails the
  upgrade.
- **Avoid name collisions in step 3.** Each `instances` key renders
  `rbln-driver-<key>`; pick keys that do not collide with existing CR
  names (e.g. `r100` when a manual `rbln-driver-rebel100` exists):

  ```yaml
  driver:
    enabled: true
    deployDefault: false
    instances:
      atom:
        image:
          tag: "3.2.2"
        nodeSelector:
          rebellions.ai/npu.family: atom
          rbln-mig/stage: "new"
      r100:
        image:
          tag: "3.3.0"
        nodeSelector:
          rebellions.ai/npu.family: rebel100
          rbln-mig/stage: "new"
  ```

- **Step 5 deletes every old CR**, helm-owned or manual.

## Appendix — worked example

The sequence below was executed end-to-end (including the abort path) on a
three-node cluster — one atom node, one rebel100 node, and one atom node
with a pre-installed host driver (out of scope by design) — migrating
chart 0.4.5 → 0.4.6. The values files it references live in
[`docs/examples/rolling-driver-migration/`](examples/rolling-driver-migration/):
[`old-values.yaml`](examples/rolling-driver-migration/old-values.yaml),
[`rbln-driver-rebel100.yaml`](examples/rolling-driver-migration/rbln-driver-rebel100.yaml)
(the manual second CR),
[`new-values.yaml`](examples/rolling-driver-migration/new-values.yaml) and
[`final-values.yaml`](examples/rolling-driver-migration/final-values.yaml).

```bash
export NS=rbln-system
CHART=oci://registry-1.docker.io/rebellions/rbln-npu-operator-chart
ATOM=worker-atom-1
R100=worker-r100-1

# Step 1 — stage labels first, then the old CRs select them
kubectl label node "$ATOM" rbln-mig/stage=old-atom
kubectl label node "$R100" rbln-mig/stage=old-r100
helm upgrade rbln-npu-operator "$CHART" --version 0.4.5 -n "$NS" -f old-values.yaml
kubectl apply -f rbln-driver-rebel100.yaml

# Step 2 — keep annotation, new CRD, pin + smd (repeated per CR)
kubectl annotate rblndriver rbln-driver helm.sh/resource-policy=keep
helm pull "$CHART" --version 0.4.6 --untar --untardir /tmp
kubectl apply --server-side --force-conflicts \
  -f /tmp/rbln-npu-operator-chart/crds/rebellions.ai_rblndrivers.yaml
for cr in rbln-driver rbln-driver-rebel100; do
  kubectl patch rblndriver "$cr" --type=merge \
    -p '{"spec":{"image":"rebellions/rbln-driver-hold"}}'
  kubectl patch rblndriver "$cr" --type=merge \
    -p '{"spec":{"smd":{"registry":"repo.rebellions.ai","image":"rebellions/rbln-smd"}}}'
done

# Step 3 — upgrade into the held state
helm upgrade rbln-npu-operator "$CHART" --version 0.4.6 -n "$NS" -f new-values.yaml

# Step 4 — canary first; the second node only after the canary verifies
kubectl label node "$ATOM" rbln-mig/stage=new --overwrite
kubectl label node "$R100" rbln-mig/stage=new --overwrite

# Step 5 — finish
kubectl delete rblndriver rbln-driver rbln-driver-rebel100
helm upgrade rbln-npu-operator "$CHART" --version 0.4.6 -n "$NS" -f final-values.yaml
kubectl label nodes -l rbln-mig/stage rbln-mig/stage-
```

Observed behavior at the checkpoints:

- **Held state** (after step 3): both old CRs report `Ready=False` with a
  message of the form `DriverImageNotFound: 1 driver pool(s) are missing
  their image in the registry: atom-ubuntu22.04-6.8.0-90-generic
  (repo.rebellions.ai/rebellions/atom/rbln-driver-hold:3.2.2-6.8.0-90-generic-ubuntu22.04);
  the check re-runs automatically; …`. The old driver pods keep running on
  both nodes and the per-CR smd pods come up in place of `rbln-daemon`.
- **Releases**: the atom node's family pod reached `1/1 Ready` about 60
  seconds after its label flip; the rebel100 node took about 3 minutes.
  The other node's old driver pod ran untouched throughout, and NPU
  resources were re-advertised on each node after its release.
- **Finish**: the family driver pods survive the final upgrade and the
  label removal unchanged (same pod names and ages), ending with
  DaemonSet pools `atom-ubuntu22.04-…` and `rebel100-ubuntu22.04-…` and
  both family CRs `Ready`.
