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

"""Check generated OperatorHub examples against their declared release images."""

import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("sync-bundle-examples.sh")
COMPONENTS = {
    "validator": "rbln-npu-operator-validator",
    "devicePlugin": "rbln-k8s-device-plugin",
    "metricsExporter": "rbln-metrics-exporter",
    "npuFeatureDiscovery": "rbln-npu-feature-discovery",
    "containerToolkit": "rbln-container-toolkit",
    "draKubeletPlugin": "dra-kubelet-plugin",
    "sandboxDevicePlugin": "sandbox-device-plugin",
    "vfioManager": "vfio-manager",
}


class BundleExamples(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.csv = Path(temp.name) / "operator.clusterserviceversion.yaml"
        old = {"registry": "docker.io", "image": "rebellions/old", "version": "v0.4.3"}
        policy = {"kind": "RBLNClusterPolicy", "metadata": {"name": "policy"}, "spec": {
            **{key: {**old, "enabled": key != "vfioManager", "imagePullPolicy": "IfNotPresent"}
               for key in COMPONENTS},
            "workloadType": "container",
        }}
        policy["spec"]["vfioManager"]["driverManager"] = dict(old)
        drivers = [{"kind": "RBLNDriver", "metadata": {"name": f"driver-{family}"}, "spec": {
            "registry": "repo.rebellions.ai", "image": "rebellions/rbln-driver", "version": version,
            "nodeSelector": {"rebellions.ai/npu.family": family}, "imagePullSecrets": ["drivercred"],
            "smd": {"registry": "repo.rebellions.ai", "image": "rebellions/rbln-smd"},
            "manager": dict(old),
        }} for family, version in (("atom", "3.4.1"), ("rebel100", "3.5.1"))]
        self.examples = [policy, *drivers]
        self.document = {
            "apiVersion": "operators.coreos.com/v1alpha1", "kind": "ClusterServiceVersion",
            "metadata": {"name": "operator.v0.6.0-rc5", "annotations": {
                "alm-examples": json.dumps(self.examples), "containerImage": "operator-image",
            }},
            "spec": {"version": "0.6.0-rc5", "relatedImages": [
                {"name": name, "image": f"docker.io/rebellions/{name}:v1.2.3@sha256:{'a' * 64}"}
                for name in [*COMPONENTS.values(), "driver-manager"]
            ]},
        }

    def write(self):
        # JSON is also valid YAML; production input is the SDK-generated CSV.
        self.csv.write_text(json.dumps(self.document))

    def run_sync(self, *args, success=True):
        result = subprocess.run(["bash", str(SCRIPT), str(self.csv), *args], text=True, capture_output=True)
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
        return result

    def read(self):
        return json.loads(subprocess.check_output(["yq", "-o=json", ".", str(self.csv)], text=True))

    def test_validator_tracks_dev_rc_and_ga_image_not_csv_version(self):
        for registry, tag in (("harbor.example:5000", "a" * 40),
                              ("harbor.example", "v0.6.0-rc5"), ("docker.io", "v0.6.0")):
            with self.subTest(registry=registry, tag=tag):
                self.document["spec"]["relatedImages"][0]["image"] = (
                    f"{registry}/team/nested/validator:{tag}@sha256:{'b' * 64}"
                )
                self.write()
                self.run_sync()
                self.run_sync("--check")
                actual = json.loads(self.read()["metadata"]["annotations"]["alm-examples"])[0]["spec"]["validator"]
                self.assertEqual(actual["registry"], registry)
                self.assertEqual(actual["image"], "team/nested/validator")
                self.assertEqual(actual["version"], tag)

    def test_updates_all_fixed_images_but_preserves_driver_versions_and_configuration(self):
        self.write()
        self.run_sync()
        result = self.read()
        self.assertEqual(result["spec"], self.document["spec"])
        actual = json.loads(result["metadata"]["annotations"]["alm-examples"])
        for key, name in COMPONENTS.items():
            image = actual[0]["spec"][key]
            self.assertEqual((image["registry"], image["image"], image["version"]),
                             ("docker.io", f"rebellions/{name}", "v1.2.3"))
            self.assertEqual(image["enabled"], self.examples[0]["spec"][key]["enabled"])
            self.assertEqual(image["imagePullPolicy"], "IfNotPresent")
        self.assertEqual(actual[0]["spec"]["vfioManager"]["driverManager"]["version"], "v1.2.3")
        for before, after in zip(self.examples[1:], actual[1:]):
            expected = copy.deepcopy(before)
            expected["spec"]["manager"] = {"registry": "docker.io", "image": "rebellions/driver-manager", "version": "v1.2.3"}
            self.assertEqual(after, expected)

    def test_check_detects_stale_examples_without_writing(self):
        self.write()
        before = self.csv.read_bytes()
        self.assertIn("do not match", self.run_sync("--check", success=False).stderr)
        self.assertEqual(self.csv.read_bytes(), before)

    def test_repeated_sync_is_a_noop(self):
        self.write()
        self.run_sync()
        before = self.csv.read_bytes()
        self.run_sync()
        self.assertEqual(self.csv.read_bytes(), before)

    def test_invalid_related_images_fail_without_partially_updating_csv(self):
        original = copy.deepcopy(self.document["spec"]["relatedImages"])
        for images in (original[1:], original + [original[0]],
                       [{**original[0], "image": "docker.io/rebellions/validator@sha256:" + "a" * 64}, *original[1:]]):
            with self.subTest(images=images):
                self.document["spec"]["relatedImages"] = images
                self.write()
                before = self.csv.read_bytes()
                self.run_sync(success=False)
                self.assertEqual(self.csv.read_bytes(), before)

    def test_missing_sample_component_fails(self):
        del self.examples[0]["spec"]["validator"]
        self.document["metadata"]["annotations"]["alm-examples"] = json.dumps(self.examples)
        self.write()
        self.run_sync(success=False)

    def test_empty_examples_fail(self):
        self.document["metadata"]["annotations"]["alm-examples"] = "[]"
        self.write()
        self.run_sync(success=False)


if __name__ == "__main__":
    unittest.main()
