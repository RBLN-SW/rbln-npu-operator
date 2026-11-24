"""Release identity and RC-to-GA invariants. No registry or cluster needed."""
import unittest
from unittest.mock import patch

import artifact


def pinned(repo, tag):
    registry, repository = repo.split("/", 1)
    return dict(registry=registry, repository=repository, tag=tag, digest="sha256:" + "a" * 64)


class ArtifactTest(unittest.TestCase):
    def test_numeric_base_and_rc_selection(self):
        tags = ["latest", "v0.9.0-rc99", "v0.10.0-rc2", "v0.10.0-rc10", "v0.10.0-rc10-arm64"]
        self.assertEqual(artifact.selected_tag(tags), "v0.10.0-rc10")
        self.assertEqual(artifact.selected_tag(tags, "0.9.0"), "v0.9.0-rc99")
        self.assertEqual(artifact.selected_tag(tags + ["v0.11.0"]), "v0.11.0")
        with self.assertRaises(ValueError):
            artifact.selected_tag(tags, "v0.12.0")

    def setUp(self):
        self.plan = dict(schemaVersion=1, sourceSha="1" * 40, phase="rc", version="v0.8.0-rc2",
                         targetRegistry="harbor.test/team", components={})
        for key in artifact.BINDINGS:
            tag = self.plan["version"] if key in artifact.OWN else "v0.6.0-rc10"
            self.plan["components"][key] = pinned("harbor.test/team/" + key.lower(), tag)
        self.plan["components"]["containerToolkit"] = pinned("docker.io/external/toolkit", "v1.2.3")

    @patch("artifact.registry_image", side_effect=pinned)
    def test_ga_uses_frozen_rc_and_preserves_external(self, registry):
        ga = artifact.promote(self.plan, "v0.8.0", "docker.io/team")
        plugin = ga["components"]["devicePlugin"]
        self.assertEqual(plugin, pinned("docker.io/team/deviceplugin", "v0.6.0"))
        self.assertEqual(ga["components"]["containerToolkit"], self.plan["components"]["containerToolkit"])
        self.assertEqual(self.plan["components"]["devicePlugin"]["tag"], "v0.6.0-rc10")
        self.assertIn("DEVICE_CONFIG_MANAGER=docker.io/team/", artifact.image_lock(ga))
        registry.assert_any_call("docker.io/team/deviceplugin", "v0.6.0")
        with self.assertRaises(ValueError):
            artifact.promote(self.plan, "v0.9.0", "docker.io/team")

    def test_ga_refuses_rebuilt_component(self):
        other = pinned("docker.io/team/deviceplugin", "v0.6.0")
        other["digest"] = "sha256:" + "b" * 64
        with patch("artifact.registry_image", return_value=other):
            with self.assertRaisesRegex(ValueError, "digest differs"):
                artifact.promote(self.plan, "v0.8.0", "docker.io/team")

    def test_incomplete_or_invalid_lock_is_not_packaged(self):
        self.plan["components"]["devicePlugin"]["tag"] = "not a tag"
        with self.assertRaises(ValueError):
            artifact.image_lock(self.plan)
        del self.plan["components"]["devicePlugin"]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            artifact.validate(self.plan)
