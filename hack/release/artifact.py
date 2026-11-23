#!/usr/bin/env python3
"""Compose a release once. Helm, OLM, validation and GA consume that exact input."""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deployments/rbln-npu-operator"
MANIFEST = ROOT / ".piper/release.yaml"
# Chart value path and OLM image-lock key. Driver container/SMD are per-family
# chart settings, not independently released operands.
BINDINGS = {
    "operator": ("operator", "OPERATOR"),
    "validator": ("validator", "VALIDATOR"),
    "devicePlugin": ("devicePlugin", "DEVICE_PLUGIN"),
    "metricsExporter": ("metricsExporter", "METRICS_EXPORTER"),
    "npuFeatureDiscovery": ("npuFeatureDiscovery", "NPU_FEATURE_DISCOVERY"),
    "containerToolkit": ("containerToolkit", "CONTAINER_TOOLKIT"),
    "draKubeletPlugin": ("draKubeletPlugin", "DRA_KUBELET_PLUGIN"),
    "deviceConfigManager": ("deviceConfigManager", "DEVICE_CONFIG_MANAGER"),
    "sandboxDevicePlugin": ("sandboxDevicePlugin", "SANDBOX_DEVICE_PLUGIN"),
    "driverManager": ("driver.manager", "DRIVER_MANAGER"),
}
OWN = {"operator", "validator"}
EXTERNAL = {"containerToolkit"}
SEMVER = re.compile(r"(v?)(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-rc(0|[1-9]\d*))?")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def output(*args):
    return subprocess.check_output(args, text=True).strip()


def read_yaml(path):
    return json.loads(output("yq", "-o=json", ".", path))


def write_json(path, data):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def pullspec(image):
    return f"{image['registry']}/{image['repository']}:{image['tag']}"


def registry_image(repo, tag):
    registry, repository = repo.split("/", 1)
    image = dict(registry=registry, repository=repository, tag=tag)
    image["digest"] = output("regctl", "image", "digest", pullspec(image))
    if not DIGEST.fullmatch(image["digest"]):
        raise ValueError(f"invalid registry digest for {pullspec(image)}")
    return image


def selected_tag(tags, override=None):
    """Select the highest base, then the highest numeric RC within that base."""
    candidates = []
    for tag in tags:
        m = SEMVER.fullmatch(tag)
        if m:
            base = tuple(map(int, m.group(2, 3, 4)))
            if override is None or tag.split("-rc")[0].lstrip("v") == override.lstrip("v"):
                candidates.append((base, int(m[5]) if m[5] is not None else -1, tag))
    if not candidates:
        raise ValueError(f"no SemVer candidate for {override or 'latest base'}")
    return max(candidates)[2]


def field(values, key):
    for part in BINDINGS[key][0].split("."):
        values = values[part]
    return values["image"]


def resolve(plan):
    if plan["phase"] != "rc" or set(plan["components"]) != OWN:
        raise ValueError("RC resolution requires the two built Operator image receipts")
    values = read_yaml(CHART / "values.yaml")
    overrides = read_yaml(MANIFEST).get("componentVersions", {})
    unknown = set(overrides) - (BINDINGS.keys() - OWN - EXTERNAL)
    if unknown:
        raise ValueError(f"unknown or external component override: {sorted(unknown)}")
    for key in BINDINGS:
        if key in OWN:
            continue
        current = field(values, key)
        registry, repository, tag = (current[k] for k in ("registry", "repository", "tag"))
        override = overrides.get(key)
        if key not in EXTERNAL and override != "stable":
            repo = plan["targetRegistry"] + "/" + repository.split("/")[-1]
            tags = output("regctl", "tag", "ls", repo).splitlines()
            tag = selected_tag(tags, override)
            image = registry_image(repo, tag)
        else:
            image = registry_image(registry + "/" + repository, tag)
        plan["components"][key] = image
    validate(plan)
    return plan


def validate(plan):
    version = plan["version"]
    if (plan.get("schemaVersion") != 1 or not re.fullmatch(r"[a-f0-9]{40}", plan["sourceSha"])
            or plan.get("phase") not in ("rc", "ga")
            or not version.startswith("v") or not SEMVER.fullmatch(version)
            or ("-rc" in version) != (plan["phase"] == "rc")):
        raise ValueError("invalid phase or release version")
    if set(plan["components"]) != BINDINGS.keys():
        raise ValueError("component lock is incomplete or contains unknown components")
    for key, image in plan["components"].items():
        if (not DIGEST.fullmatch(image["digest"]) or
                not re.fullmatch(r"[a-zA-Z0-9.:-]+", image["registry"]) or
                not re.fullmatch(r"[a-z0-9._/-]+", image["repository"]) or
                not re.fullmatch(r"[A-Za-z0-9_.-]+", image["tag"])):
            raise ValueError(f"invalid image for {key}")
        if key in OWN and image["tag"] != version:
            raise ValueError(f"{key} tag differs from release version")


def promote(plan, version, public):
    validate(plan)
    if plan["phase"] != "rc" or version != plan["version"].split("-rc")[0]:
        raise ValueError("GA must match its selected RC")
    ga = copy.deepcopy(plan)
    ga.update(version=version, phase="ga", targetRegistry=public, promotedFrom=plan["version"])
    for key, image in ga["components"].items():
        if key in EXTERNAL:
            repo = f"{image['registry']}/{image['repository']}"
        else:
            repo = public + "/" + image["repository"].split("/")[-1]
        tag = image["tag"].split("-rc")[0] if key not in EXTERNAL else image["tag"]
        if key in OWN:
            # The release runner copies these exact source digests after this preflight.
            image["registry"], image["repository"] = repo.split("/", 1)
            image["tag"] = tag
        else:
            actual = registry_image(repo, tag)
            if actual["digest"] != image["digest"]:
                raise ValueError(f"GA digest differs from tested RC: {key} {repo}:{tag}")
            ga["components"][key] = actual
    validate(ga)
    return ga


def image_lock(plan):
    validate(plan)
    return "".join(f"{olm}={pullspec(plan['components'][key])}@{plan['components'][key]['digest']}\n"
                   for key, (_, olm) in BINDINGS.items())


def package_chart(plan, dest):
    with tempfile.TemporaryDirectory() as tmp:
        chart = Path(tmp) / "chart"
        shutil.copytree(CHART, chart, ignore=shutil.ignore_patterns("charts"))
        overlay = {}
        for key, image in plan["components"].items():
            cursor = overlay
            for part in BINDINGS[key][0].split("."):
                cursor = cursor.setdefault(part, {})
            cursor["image"] = {k: image[k] for k in ("registry", "repository", "tag")}
        override = Path(tmp) / "override.json"
        write_json(override, overlay)
        with (chart / "values.yaml").open("w") as out:
            subprocess.run(["yq", "eval-all", ". as $doc ireduce ({}; . * $doc)",
                            CHART / "values.yaml", override], stdout=out, check=True)
        subprocess.run(["make", "helm-package", f"CHART_DIR={chart}",
                        f"CHART_DIST_DIR={dest}", f"VERSION={plan['version']}"], cwd=ROOT, check=True)
    return dest / f"rbln-npu-operator-chart-{plan['version'][1:]}.tgz"


def build(name, kind, plan, result):
    validate(plan)
    if output("git", "-C", ROOT, "rev-parse", "HEAD") != plan["sourceSha"]:
        raise ValueError("release checkout differs from frozen source SHA")
    dest = Path(result).resolve().parent
    manifest = read_yaml(MANIFEST)
    spec = next((a for a in manifest["artifacts"] if (a["name"], a["kind"]) == (name, kind)), None)
    if not spec:
        raise ValueError(f"undeclared release artifact {name}/{kind}")
    if kind == "helm-chart":
        package = package_chart(plan, dest)
        built = {"type": "file", "path": str(package)}
    else:
        tag = plan["version"]
        ref = f"{plan['targetRegistry']}/{spec['repository']}:{tag}"
        remote = subprocess.run(["regctl", "image", "digest", ref], capture_output=True, text=True)
        if remote.returncode == 0 or not re.search(r"not found|manifest unknown|404", remote.stderr, re.I):
            raise ValueError(f"cannot safely publish {ref}: {remote.stdout.strip()} {remote.stderr.strip()}")
        # Bundle generation edits config/, bundle/ and CRDs in place. Run the
        # repository's recipes in a disposable copy; return the very manifests
        # that were packaged for the GA certification PR.
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
            for entry in tracked.split(b"\0"):
                if not entry:
                    continue
                rel = Path(os.fsdecode(entry))
                (work / rel.parent).mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / rel, work / rel, follow_symlinks=False)
            if kind == "olm-bundle":
                lock = dest / "component-digests.env"
                lock.write_text(image_lock(plan))
                subprocess.run(["bash", "hack/release/prepare-bundle.sh", lock, tag, plan["phase"]],
                               cwd=work, check=True)
                target, var = "build-bundle-image", "BUNDLE_IMAGE"
            elif kind == "olm-catalog":
                bundle = plan["artifacts"]["bundle"]
                if bundle.get("type") != "registry" or not DIGEST.fullmatch(bundle["digest"]):
                    raise ValueError("catalog requires a pinned bundle")
                subprocess.run(["make", "catalog", f"VERSION={tag}",
                                f"CATALOG_BUNDLE={bundle['ref']}@{bundle['digest']}"], cwd=work, check=True)
                target, var = "catalog-build-image", "CATALOG_IMAGE"
            else:
                raise ValueError(f"unsupported artifact kind {kind}")
            subprocess.run(["make", target, f"{var}={ref}", "PLATFORM=linux/amd64",
                            "PUSH_ON_BUILD=true"], cwd=work, check=True)
            if kind == "olm-bundle":
                shutil.copytree(work / "bundle", dest / "olm-bundle", dirs_exist_ok=True)
        digest = output("regctl", "image", "digest", ref)
        if not DIGEST.fullmatch(digest):
            raise ValueError(f"invalid registry digest: {ref}")
        built = {"type": "registry", "ref": ref, "digest": digest}
    write_json(result, {"schemaVersion": 1, "name": name, "kind": kind, "output": built})


def metadata(plan, catalog, path):
    validate(plan)

    def pinned(item):
        return item["ref"] + "@" + item["digest"]

    chart = plan["artifacts"]["chart"]
    operator, validator = (plan["components"][k] for k in ("operator", "validator"))
    version = plan["version"].removeprefix("v")
    write_json(path, {
        "schema_version": 1, "kind": plan["phase"], "package": "rbln-npu-operator", "channel": "stable",
        "source": {"repository": "RBLN-SW/rbln-npu-operator", "commit": plan["sourceSha"]},
        "csv": {"name": "rbln-npu-operator.v" + version, "version": version},
        "images": {"operator": pullspec(operator) + "@" + operator["digest"],
                   "validator": pullspec(validator) + "@" + validator["digest"],
                   "bundle": pinned(plan["artifacts"]["bundle"]), "catalog": pinned(catalog["output"])},
        "chart": {"ref": "oci://" + chart["ref"].rsplit(":", 1)[0], "version": version,
                  "image": pinned(chart), "source_path": "deployments/rbln-npu-operator"},
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command, names in {"resolve": ["input", "result"], "promote": ["input", "version", "public", "result"],
                           "lock": ["input", "result"], "verify": ["input"],
                           "metadata": ["input", "catalog", "result"],
                           "build": ["name", "kind", "input", "result"]}.items():
        sub = commands.add_parser(command)
        for name in names:
            sub.add_argument(name)
    a = parser.parse_args()
    plan = json.loads(Path(a.input).read_text())
    if a.command == "resolve":
        write_json(a.result, resolve(plan))
    elif a.command == "promote":
        write_json(a.result, promote(plan, a.version, a.public))
    elif a.command == "lock":
        Path(a.result).write_text(image_lock(plan))
    elif a.command == "metadata":
        metadata(plan, json.loads(Path(a.catalog).read_text()), a.result)
    elif a.command == "verify":
        validate(plan)
        for key, image in plan["components"].items():
            if output("regctl", "image", "digest", pullspec(image)) != image["digest"]:
                raise ValueError(f"registry digest changed: {key} {pullspec(image)}")
    else:
        build(a.name, a.kind, plan, a.result)


if __name__ == "__main__":
    main()
