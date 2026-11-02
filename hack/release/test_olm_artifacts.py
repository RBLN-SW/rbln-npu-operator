# Copyright 2026 Rebellions Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise OLM publishing without a registry or cluster (requires yq and jq).

The real scripts and YAML edits run in a temporary checkout. Git, make,
operator-sdk and regctl are local fakes; the RNPM HTTP client is intercepted.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40
KEYS = (
    "OPERATOR", "VALIDATOR", "DEVICE_PLUGIN", "METRICS_EXPORTER",
    "NPU_FEATURE_DISCOVERY", "VFIO_MANAGER", "CONTAINER_TOOLKIT",
    "DRA_KUBELET_PLUGIN", "SANDBOX_DEVICE_PLUGIN", "DRIVER_MANAGER",
)

FAKE_TOOL = r'''#!/usr/bin/env python3
import hashlib, json, os, pathlib, sys
tool, args = pathlib.Path(sys.argv[0]).name, sys.argv[1:]
root = pathlib.Path(os.environ["TEST_ROOT"])
with (root / "calls.jsonl").open("a") as f:
    f.write(json.dumps([tool, *args]) + "\n")
state_path = root / "registry.json"
state = json.loads(state_path.read_text())
digest = lambda ref: "sha256:" + hashlib.sha256(ref.encode()).hexdigest()
if tool == "git":
    print("a" * 40)
elif tool == "regctl" and args[:2] == ["image", "digest"]:
    ref = args[2]
    if os.environ.get("FAIL_RESOLVE") and "/metrics:" in ref:
        sys.exit(1)
    if "-bundle:" in ref or "-catalog:" in ref:
        if ref not in state:
            sys.exit(1)
        print(state[ref])
    else:
        print(digest(ref))
elif tool == "regctl" and args[:2] == ["image", "copy"]:
    state[args[3]] = args[2].split("@", 1)[1]
    state_path.write_text(json.dumps(state))
elif tool == "make":
    target = args[0]
    opts = dict(arg.split("=", 1) for arg in args[1:] if "=" in arg)
    if target == "bundle":
        version = opts["VERSION"].removeprefix("v")
        csv = root / "bundle/manifests/rbln-npu-operator.clusterserviceversion.yaml"
        csv.parent.mkdir(parents=True, exist_ok=True)
        csv.write_text(f"metadata:\n  name: rbln-npu-operator.v{version}\nspec:\n  version: {version}\n")
        metadata = root / "bundle/metadata/annotations.yaml"
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.write_text("annotations: {}\n")
    if target in ("build-bundle-image", "catalog-build-image"):
        if target == os.environ.get("FAIL_BUILD"):
            sys.exit(1)
        ref = opts["BUNDLE_IMAGE" if target == "build-bundle-image" else "CATALOG_IMAGE"]
        state[ref] = digest(ref)
        state_path.write_text(json.dumps(state))
elif tool == "curl":
    payload = args[args.index("-d") + 1]
    (root / "webhook.json").write_text(payload)
    print('{"web_url":"https://ci.example.test/build/1"}')
elif tool != "operator-sdk":
    sys.exit("unexpected tool: " + tool)
'''

RNPM_HTTP_STUB = r'''"""Intercept every HTTP request made by the real RNPM helper."""
import io, json, os, pathlib, urllib.parse, urllib.request
root = pathlib.Path(os.environ["TEST_ROOT"])
config = json.loads((root / ".github/scripts/rnpm-rc.json").read_text())
base = "/api/bundles/" + config["bundle_id"]
pipeline = {
    "id": "fixture-pipeline", "key": "containerd-full", "enabled": True,
    "cluster_id": "fixture-cluster", "timeout_minutes": 240,
    "env": {"SENTINEL_ATOM_DRIVER_VERSION": "hardware-pin",
            "SENTINEL_OPERATOR_VERSION": "dev"},
    "chart": {"source": "clone", "repo": "saved-chart", "ref": "main", "path": "chart"},
    "scenarios": [{"family": "components/dra", "subtests": []}],
}
def fake_open(self, request, **kwargs):
    url = urllib.parse.urlsplit(request.full_url)
    if url.scheme != "https" or url.netloc != "rnpm.example.test":
        raise AssertionError("unexpected external URL: " + request.full_url)
    method, path = request.get_method(), url.path
    payload = json.loads(request.data) if request.data is not None else None
    with (root / "rnpm-calls.jsonl").open("a") as out:
        out.write(json.dumps({"method": method, "path": path, "payload": payload}) + "\n")
    if method == "GET" and path == base:
        result = {"repository_id": "fixture-repository", "ob_pipeline": config["ob_pipeline"],
                  "pipelines": [{"id": pipeline["id"]}]}
    elif method == "GET" and path == "/api/repositories/fixture-repository":
        owner, name = config["execution_repository"].split("/")
        result = {"owner": owner, "name": name}
    elif method == "GET" and path == "/api/pipelines/" + pipeline["id"]:
        result = pipeline
    elif method == "POST" and path == base + "/preview":
        result = {"yaml": "steps: []\n", "warnings": []}
    elif method == "GET" and path in (base + "/bootstrap", base + "/ob/bootstrap"):
        result = {"matches": True}
    elif method == "GET" and path == base + "/builds":
        result = []
    elif method == "POST" and path == base + "/trigger":
        (root / "rnpm-request.json").write_text(json.dumps(payload))
        result = {"number": 42, "url": "https://ci.example.test/build/42",
                  "env": {"RNPM_RUN": "fixture-run"}}
    elif method == "GET" and path == base + "/builds/42":
        result = {"number": 42, "state": os.environ.get("TEST_RNPM_STATE", "passed")}
    elif method == "GET" and path == "/api/runs/fixture-run":
        payload = json.loads((root / "rnpm-request.json").read_text())
        state = os.environ.get("TEST_RNPM_STATE", "passed")
        result = {"id": "fixture-run", "ob_build": 42, "status_delivery": {
            "target": payload["overrides"]["status_target"], "error": "",
            "sent": {"rc-validation": "success" if state == "passed" else "failure"}}}
    else:
        raise AssertionError("unexpected RNPM request: " + method + " " + path)
    return io.BytesIO(json.dumps(result).encode())
urllib.request.OpenerDirector.open = fake_open
'''


class OLMPublishing(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("lib.sh", "resolve-component-digests.sh", "publish-olm.sh", "validate-rc.sh"):
            dest = self.root / "hack/release" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / "hack/release" / name, dest)
        for name in ("config/manager/manager.yaml", "versions.mk",
                     ".github/scripts/rnpm_rc.py", ".github/scripts/rnpm-rc.json"):
            dest = self.root / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, dest)
        tools = self.root / "tools"
        tools.mkdir()
        for name in ("git", "make", "regctl", "operator-sdk", "curl"):
            path = tools / name
            path.write_text(FAKE_TOOL)
            path.chmod(0o755)
        (self.root / "registry.json").write_text("{}")
        self.lock = self.root / "component-digests.env"
        self.images = {key: f"registry.test/{key.lower()}:v1@sha256:{'b' * 64}" for key in KEYS}
        self.lock.write_text("".join(f"{key}={ref}\n" for key, ref in self.images.items()))
        self.env = {
            "PATH": f"{tools}:{os.environ['PATH']}", "HOME": os.environ["HOME"],
            "TEST_ROOT": str(self.root), "VERSION": "v1.2.3-rc1", "OLM_IMAGE_TAG": "v1.2.3-rc1",
            "REGISTRY": "registry.test/team", "HELM_CHART": "oci://registry.test/team/chart",
            "HELM_CHART_VERSION": "1.2.3-rc1", "GITHUB_OUTPUT": str(self.root / "outputs"),
            "RNPM_URL": "https://rnpm.example.test", "RNPM_TOKEN": "offline-test-token",
            "PYTHONPATH": str(self.root),
        }
        (self.root / "sitecustomize.py").write_text(RNPM_HTTP_STUB)

    def run_script(self, script, *args, success=True):
        result = subprocess.run(
            ["bash", str(self.root / "hack/release" / script), *map(str, args)],
            env=self.env, text=True, capture_output=True,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def calls(self):
        path = self.root / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_rc_pins_bundle_catalog_operator_and_records_chart(self):
        self.run_script("publish-olm.sh", self.lock)
        metadata = json.loads((self.root / "dist/olm/olm-artifacts.json").read_text())
        self.assertEqual(metadata["kind"], "rc")
        self.assertEqual(metadata["source"]["commit"], SHA)
        self.assertEqual(metadata["csv"], {"name": "rbln-npu-operator.v1.2.3-rc1", "version": "1.2.3-rc1"})
        self.assertEqual(metadata["images"]["operator"], self.images["OPERATOR"])
        self.assertEqual(metadata["chart"]["version"], "1.2.3-rc1")
        self.assertIn("@sha256:", metadata["chart"]["image"])
        calls = self.calls()
        render = next(c for c in calls if c[:2] == ["make", "catalog"])
        self.assertIn("CATALOG_BUNDLE=" + metadata["images"]["bundle"], render)
        generate = next(c for c in calls if c[:2] == ["make", "bundle"])
        self.assertIn("IMAGE=" + self.images["OPERATOR"], generate)
        self.assertFalse(any(c[:3] == ["regctl", "image", "copy"] for c in calls))
        manager = (self.root / "config/manager/manager.yaml").read_text()
        for ref in self.images.values():
            self.assertIn(ref, manager)
        self.assertIn("catalog_image=" + metadata["images"]["catalog"], (self.root / "outputs").read_text())

    def test_dev_aliases_reference_snapshots_and_csv_stays_compatible(self):
        self.env.update(VERSION="v0.0.0-dev", OLM_IMAGE_TAG="sha-" + SHA, HELM_CHART_VERSION="0.0.0-dev")
        self.run_script("publish-olm.sh", self.lock)
        metadata = json.loads((self.root / "dist/olm/olm-artifacts.json").read_text())
        self.assertEqual(metadata["csv"]["version"], "0.0.0-dev")
        for kind in ("bundle", "catalog"):
            self.assertIn(f":sha-{SHA}@sha256:", metadata["images"][kind])
            self.assertIn(["regctl", "image", "copy", metadata["images"][kind],
                           f"registry.test/team/rbln-npu-operator-{kind}:dev"], self.calls())

    def test_failed_catalog_does_not_advance_dev_aliases(self):
        self.env.update(VERSION="v0.0.0-dev", OLM_IMAGE_TAG="sha-" + SHA, FAIL_BUILD="catalog-build-image")
        self.run_script("publish-olm.sh", self.lock, success=False)
        self.assertFalse(any(c[:3] == ["regctl", "image", "copy"] for c in self.calls()))

    def test_missing_or_duplicate_digest_prevents_publication(self):
        for contents in ("OPERATOR=registry.test/operator:v1\n", self.lock.read_text() + "OPERATOR=duplicate\n"):
            with self.subTest(contents=contents[:45]):
                self.lock.write_text(contents)
                self.run_script("publish-olm.sh", self.lock, success=False)
                self.assertFalse(any(c[0] == "make" for c in self.calls()))

    def test_existing_snapshot_is_not_overwritten(self):
        ref = "registry.test/team/rbln-npu-operator-bundle:v1.2.3-rc1"
        (self.root / "registry.json").write_text(json.dumps({ref: "sha256:" + "b" * 64}))
        result = self.run_script("publish-olm.sh", self.lock, success=False)
        self.assertIn("refusing to overwrite", result.stderr)
        self.assertFalse(any(c[0] == "make" for c in self.calls()))

    def test_ga_is_not_published(self):
        self.env.update(VERSION="v1.2.3", OLM_IMAGE_TAG="v1.2.3")
        self.run_script("publish-olm.sh", self.lock, success=False)
        self.assertEqual(self.calls(), [])

    def test_resolve_stops_when_an_operand_cannot_be_resolved(self):
        values = self.root / "values.yaml"
        values.write_text("devicePlugin:\n  image: {registry: registry.test, repository: plugin, tag: v1}\n"
                          "metricsExporter:\n  image: {registry: registry.test, repository: metrics, tag: v1}\n")
        self.env["FAIL_RESOLVE"] = "true"
        result = self.run_script("resolve-component-digests.sh", "registry.test/operator:v1",
                                 "registry.test/validator:v1", values, success=False)
        self.assertIn("DEVICE_PLUGIN=registry.test/plugin:v1@sha256:", result.stdout)
        self.assertNotIn("METRICS_EXPORTER=", result.stdout)

    def test_rc_trigger_passes_pinned_catalog_and_retains_manual_tag_fallback(self):
        pinned = "registry.test/team/catalog:v1.2.3-rc1@sha256:" + "b" * 64
        self.env["SENTINEL_CATALOG_IMAGE"] = pinned
        self.run_script("validate-rc.sh", "v1.2.3-rc1")
        payload = json.loads((self.root / "rnpm-request.json").read_text())
        overrides = payload["overrides"]
        self.assertEqual((payload["branch"], payload["commit"]), ("dev", "HEAD"))
        self.assertEqual(overrides["build"], {"enabled": False})
        self.assertTrue(overrides["force_env"])
        self.assertEqual(overrides["status_target"],
                         {"repository": "RBLN-SW/rbln-npu-operator", "sha": SHA})
        self.assertEqual(overrides["commit_status"],
                         {"enabled": True, "context": "rc-validation"})
        self.assertEqual(overrides["env"]["SENTINEL_CATALOG_IMAGE"], pinned)
        self.assertEqual(overrides["env"]["SENTINEL_HELM_CHART_VERSION"], "1.2.3-rc1")
        self.assertEqual(overrides["env"]["SENTINEL_OPERATOR_VERSION"], "")
        member = overrides["members"][0]["pipeline"]
        self.assertEqual(member["chart"], {
            "source": "oci", "oci": "oci://harbor.k8s.rebellions.in/rebellions/rbln-npu-operator-chart",
            "version": "1.2.3-rc1",
        })
        self.assertEqual(member["env"]["SENTINEL_ATOM_DRIVER_VERSION"], "hardware-pin")
        self.assertIn("build_url=https://ci.example.test/build/42", (self.root / "outputs").read_text())
        self.assertIn("run_id=fixture-run", (self.root / "outputs").read_text())
        del self.env["SENTINEL_CATALOG_IMAGE"]
        self.run_script("validate-rc.sh", "v1.2.3-rc1")
        payload = json.loads((self.root / "rnpm-request.json").read_text())
        self.assertEqual(payload["overrides"]["env"]["SENTINEL_CATALOG_IMAGE"],
                         "harbor.k8s.rebellions.in/rebellions/rbln-npu-operator-catalog:v1.2.3-rc1")

    def test_rc_validation_returns_rnpm_failure(self):
        self.env["TEST_RNPM_STATE"] = "failed"
        result = self.run_script("validate-rc.sh", "v1.2.3-rc1", success=False)
        self.assertIn("RNPM/OB result: failed", result.stdout)
        self.assertIn("rc-validation delivery: failure", result.stdout)


if __name__ == "__main__":
    unittest.main()
