# Rolling node-by-node migration to family-scoped driver DaemonSets

This guide upgrades the operator across the **flat → family-scoped driver
DaemonSet transition** while controlling exactly **when each node** goes
through its driver reinstall, instead of letting every node switch over at
the same time.

## Background

Family-scoped pools change the driver DaemonSet's identity (its name and
immutable label selector embed the pool name, which now includes the NPU
family). The old flat DaemonSet therefore cannot be updated in place — the
operator replaces it: the flat DaemonSet is reaped and per-family DaemonSets
are created.

The operator's built-in gate makes this replacement **fail-safe** (nothing is
reaped until every owned node carries a usable `rebellions.ai/npu.family`
label *and* every family pool's image is confirmed in the registry), but not
**rolling**: once the gate opens, the switchover happens on all nodes of a
pool at once, and each node's new driver pod runs a full reinstall (pause
components → evict NPU pods → unload module → install).

This guide adds rolling control with a scheduling taint: the new family
DaemonSet pods carry no tolerations by default, so a `NoSchedule` taint keeps
them `Pending` per node until you remove it. Removing the taint from a node
is the per-node "go" button.

> **Scope**: single `RBLNDriver` CR (`rbln-driver`, the default install)
> upgrading from a pre-family operator release. Adjust names if your CR is
> named differently.

## Prerequisites

```bash
export NS=<operator-namespace>          # helm release namespace
```

1. **NFD is installed and healthy** (the operator manages a `NodeFeatureRule`
   named `rbln-npu-family`; NFD applies it):

   ```bash
   kubectl get crd nodefeaturerules.nfd.k8s-sigs.io
   ```

2. **Family-scoped driver images are published.** The composed path per pool
   is:

   ```
   <registry>/<repo-prefix>/<family>/<leaf>:<version>-<kernel>-<os>
   # e.g. repo.rebellions.ai/rebellions/atom/rbln-driver:3.2.2-5.15.0-100-generic-ubuntu22.04
   ```

   Verify each (family, os, kernel) combination present in your fleet:

   ```bash
   crane manifest repo.rebellions.ai/rebellions/atom/rbln-driver:3.2.2-5.15.0-100-generic-ubuntu22.04 > /dev/null && echo OK
   ```

   If you are unsure of the exact paths, you can also upgrade first (step 2):
   the gate holds the flat DaemonSet safely, and the CR's `Ready` condition
   (`reason: DriverImageNotFound`) lists the exact missing image paths to
   publish.

3. **No broad tolerations on the driver pods.** The taint gate only works if
   the driver pods do not tolerate arbitrary taints. Confirm `tolerations`
   under the chart's `driver:` block and `podDefaults:` do not include an
   `operator: Exists` catch-all. If they do, pick a taint key their
   tolerations don't match.

4. **Snapshot the healthy starting state:**

   ```bash
   kubectl get nodes -l rebellions.ai/npu.deploy.driver=true
   kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver \
     -L rebellions.ai/driver-node-pool
   kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver -o wide
   ```

   All driver pods should be `Running` and `Ready` before you start.

## Step 1 — Taint every driver node (before `helm upgrade`)

```bash
kubectl taint nodes -l rebellions.ai/npu.deploy.driver=true \
  rebellions.ai/driver-migration=pending:NoSchedule
```

Notes:

- `NoSchedule` does **not** evict running pods — the old driver pods and your
  NPU workloads keep running.
- The taint blocks **all** new pod scheduling on these nodes (equivalent to a
  cordon for new pods). Plan the migration inside a maintenance window, or
  work through the batches promptly.
- Nodes labeled `npu.deploy.driver=pre-installed` are out of scope (they run
  no operator-managed driver) and are intentionally not matched by the
  selector above.

## Step 2 — Upgrade the operator and let the switchover happen

```bash
helm upgrade rbln-npu-operator <chart-ref> -n "$NS" -f <your-values.yaml>
```

Watch the family labels being applied by NFD (may take up to NFD's sync
period):

```bash
kubectl get nodes -l rebellions.ai/npu.deploy.driver=true \
  -L rebellions.ai/npu.family -L rebellions.ai/npu.driver.owner
```

Once **every** node carries a family label and the images verify, the
operator performs the switchover pass:

```bash
kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  -L rebellions.ai/driver-node-pool -w
```

Expected state after this pass — this is the **taint-held plateau**:

- The flat DaemonSet (pool without a family prefix) is deleted; its pods
  terminate on all nodes. The kernel module **stays loaded** and existing NPU
  workloads keep running — deleting a driver pod does not unload the driver.
- Family DaemonSets exist with `DESIRED = <node count>` but `READY = 0`; all
  their pods are `Pending` (blocked by the taint). This is expected.
- The `RBLNDriver` CR reports `Ready=False` (`DriverPoolNotReady`) until the
  rollout completes.

If instead the flat DaemonSet is still present and the CR shows
`DriverFamilyLabelMissing` or `DriverImageNotFound`, the gate is holding:
fix the named label/image gap (the condition message lists specifics) and the
switchover proceeds automatically. Nothing has been torn down at that point.

**Do not reboot tainted nodes during the plateau** — there is no driver pod
to reinstall the module after a reboot until the node is untainted.

## Step 3 — Migrate a canary node

```bash
NODE=<canary-node>
kubectl taint node "$NODE" rebellions.ai/driver-migration-
```

The pending driver pod schedules onto the node and runs the handover
**on this node only**: the `k8s-driver-manager` init container pauses the
node's RBLN components and evicts NPU pods, unloads the kernel module, and
the driver container installs the family-scoped driver. Expect the node's
NPU workloads to be evicted and rescheduled — this is the per-node
disruption window (a few minutes).

Watch it complete:

```bash
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  --field-selector spec.nodeName="$NODE" -w
```

Verify before proceeding:

```bash
# driver pod on the node is 1/1 Ready (startup probe = module + install marker)
kubectl get pods -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  --field-selector spec.nodeName="$NODE"

# NPU resources are advertised again
kubectl get node "$NODE" -o jsonpath='{.status.allocatable}' | tr ',' '\n' | grep rebellions.ai
```

## Step 4 — Roll through the remaining nodes in batches

```bash
# example: batches of 3
for NODE in worker-2 worker-3 worker-4; do
  kubectl taint node "$NODE" rebellions.ai/driver-migration-
done

kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver -w
```

Wait for the family DaemonSets' `READY` count to absorb each batch before
starting the next one. Batch size is your blast-radius budget — each node in
a batch goes through its NPU-workload eviction concurrently.

## Step 5 — Final verification

```bash
# no migration taints remain
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.taints[*].key}{"\n"}{end}' \
  | grep driver-migration && echo "TAINTS REMAIN" || echo "clean"

# every family DaemonSet fully ready, flat DaemonSet gone
kubectl get ds -n "$NS" -l app.kubernetes.io/component=rbln-driver \
  -L rebellions.ai/driver-node-pool

# CR is Ready
kubectl get rblndriver rbln-driver \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")]}'
```

## Failure handling

- **A node's new driver pod fails (CrashLoop / install error):** stop
  untainting further nodes; every still-tainted node keeps running the old
  driver module untouched. Investigate the failing node in isolation.
- **Abort mid-migration (`helm rollback`):** roll the operator back, then
  untaint the remaining nodes. The previous operator recreates the flat
  DaemonSet; on nodes whose handover never ran, the stored driver-config
  digest still matches, so the returning flat pod skips the reinstall and
  recovery is immediate. Nodes already migrated go through one reinstall back
  to the flat driver.
- **Images turn out to be missing after Step 1:** safe — the gate keeps the
  flat DaemonSet running and the CR names the missing paths
  (`DriverImageNotFound`). Publish them (or fix `spec.imagePullSecrets`; the
  condition warns when configured pull secrets are absent) and the switchover
  resumes within ~5 minutes.

## After this migration

This taint procedure is only needed for the one-time flat → family
transition. Subsequent driver upgrades have first-class rolling mechanisms:

- **Version bumps within a pool** keep the DaemonSet identity; the upgrade
  controller paces them per node via `upgradePolicy.maxParallelUpgrades`
  (cordon → drain → replace → uncordon).
- **Canary/wave rollouts** can now use multiple `RBLNDriver` CRs: family
  DaemonSets select on the operator-managed `rebellions.ai/npu.driver.owner`
  label, so relabeling a node moves it atomically between CRs — the old CR's
  pod leaves and the new CR's pod arrives, one node at a time.
