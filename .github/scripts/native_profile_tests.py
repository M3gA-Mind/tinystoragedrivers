"""Native instrumentation follows the actual canonical checkout directory."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

from native_provenance import profile_directory


class NativeProfileDirectory(unittest.TestCase):
    def test_worktree_profile_list_is_used_instead_of_canonical_basename(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "schema-owner"
            target = root / "target/llvm-cov-target"
            target.mkdir(parents=True)
            profiles = target / "profiles"
            (target / "schema-owner-profraw-list").write_text(str(profiles / "run.profraw") + "\n")
            (target / "tinystoragedrivers-profraw-list").write_text(str(target / "stale/run.profraw"))
            self.assertEqual(profile_directory(root), profiles)

    def test_empty_or_ambiguous_current_profile_lists_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "schema-owner"
            target = root / "target/llvm-cov-target"
            target.mkdir(parents=True)
            inventory = target / "schema-owner-profraw-list"
            for contents in ["", "a/run.profraw\nb/run.profraw\n"]:
                inventory.write_text(contents)
                with self.assertRaises(RuntimeError):
                    profile_directory(root)


if __name__ == "__main__":
    unittest.main()
