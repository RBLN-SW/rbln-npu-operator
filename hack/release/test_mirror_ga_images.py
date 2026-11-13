"""Verify GA Harbor tags against the release kit's promoted image digests."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("mirror-ga-images.sh")
REGISTRY = "harbor.example/rebellions"
IMAGES = ("rbln-npu-operator", "rbln-npu-operator-validator")
DIGESTS = ("sha256:" + "a" * 64, "sha256:" + "b" * 64)
TAG = "v1.2.3"

REGCTL = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
root = pathlib.Path(os.environ["TEST_ROOT"])
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as out:
    out.write(json.dumps(args) + "\n")
state = json.loads((root / "registry.json").read_text())
if args[:2] == ["image", "digest"]:
    if args[2] not in state:
        sys.exit(1)
    print(state[args[2]])
elif args[:2] == ["tag", "ls"]:
    if os.environ.get("FAIL_TAG_LIST"):
        sys.exit("registry unavailable")
    prefix = args[2] + ":"
    print("\n".join(ref[len(prefix):] for ref in state if ref.startswith(prefix)))
elif args[:2] == ["image", "copy"]:
    state[args[3]] = "sha256:" + "c" * 64 if os.environ.get("BREAK_COPY") else state[args[2]]
    (root / "registry.json").write_text(json.dumps(state))
else:
    sys.exit("unexpected regctl call")
'''


class MirrorGAImages(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        tool = self.root / "regctl"
        tool.write_text(REGCTL)
        tool.chmod(0o755)
        self.state = {f"{REGISTRY}/{image}@{digest}": digest for image, digest in zip(IMAGES, DIGESTS)}
        # A moved RC tag must not change which images are promoted.
        self.state.update({f"{REGISTRY}/{image}:{TAG}-rc9": "sha256:" + "d" * 64 for image in IMAGES})
        self.save()
        self.env = {**os.environ, "PATH": f"{self.root}:{os.environ['PATH']}",
                    "TEST_ROOT": str(self.root), "HARBOR_REGISTRY": REGISTRY,
                    "PROMOTED": "".join(f"{image}={digest}\n" for image, digest in zip(IMAGES, DIGESTS))}

    def save(self):
        (self.root / "registry.json").write_text(json.dumps(self.state))

    def copies(self):
        log = self.root / "calls.jsonl"
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return [call for call in calls if call[:2] == ["image", "copy"]]

    def run_mirror(self, tag=TAG, success=True):
        result = subprocess.run(["bash", str(SCRIPT), tag], env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
        return result

    def test_copies_exact_promoted_digests_to_both_ga_tags(self):
        self.run_mirror()
        self.assertEqual(self.copies(), [
            ["image", "copy", f"{REGISTRY}/{image}@{digest}", f"{REGISTRY}/{image}:{TAG}"]
            for image, digest in zip(IMAGES, DIGESTS)
        ])
        state = json.loads((self.root / "registry.json").read_text())
        for image, digest in zip(IMAGES, DIGESTS):
            self.assertEqual(state[f"{REGISTRY}/{image}:{TAG}"], digest)
        self.assertFalse(any(ref.endswith(":latest") for ref in state))

    def test_rerun_with_matching_ga_tags_does_not_copy_again(self):
        self.run_mirror()
        copies = self.copies()
        self.run_mirror()
        self.assertEqual(self.copies(), copies)

    def test_conflicting_second_image_prevents_all_writes(self):
        self.state[f"{REGISTRY}/{IMAGES[1]}:{TAG}"] = "sha256:" + "c" * 64
        self.save()
        result = self.run_mirror(success=False)
        self.assertIn("different digest", result.stderr)
        self.assertEqual(self.copies(), [])

    def test_missing_duplicate_or_invalid_digest_prevents_all_writes(self):
        original = self.env["PROMOTED"]
        for promoted in (original.splitlines()[0], original + original.splitlines()[1],
                         original.replace(DIGESTS[1], "sha256:invalid")):
            with self.subTest(promoted=promoted):
                self.env["PROMOTED"] = promoted
                self.run_mirror(success=False)
                self.assertEqual(self.copies(), [])

    def test_rc_and_dev_tags_are_rejected(self):
        for tag in ("v1.2.3-rc9", "dev", "1.2.3"):
            with self.subTest(tag=tag):
                self.run_mirror(tag, success=False)
                self.assertEqual(self.copies(), [])

    def test_registry_failure_is_not_treated_as_an_absent_tag(self):
        self.env["FAIL_TAG_LIST"] = "true"
        self.run_mirror(success=False)
        self.assertEqual(self.copies(), [])

    def test_digest_mismatch_after_copy_fails(self):
        self.env["BREAK_COPY"] = "true"
        result = self.run_mirror(success=False)
        self.assertIn("digest changed", result.stderr)
        self.assertEqual(len(self.copies()), 1)


if __name__ == "__main__":
    unittest.main()
