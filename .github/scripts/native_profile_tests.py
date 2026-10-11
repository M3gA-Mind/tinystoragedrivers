"""Native instrumentation must use the inventory of this checkout."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True

from native_provenance import profile_directory


class ReachedNativeExecution(Exception):
    pass


class NativeProfileInventory(unittest.TestCase):
    def test_empty_ambiguous_or_missing_current_inventory_refuses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "schema-owner"
            target = root / "target/llvm-cov-target"
            target.mkdir(parents=True)
            inventory = target / "schema-owner-profraw-list"
            (target / "tinystoragedrivers-profraw-list").write_text("stale/run.profraw\n")
            with self.assertRaises(FileNotFoundError):
                profile_directory(root)
            for contents in ["", "a/run.profraw\nb/run.profraw\n"]:
                inventory.write_text(contents)
                with self.assertRaises(RuntimeError):
                    profile_directory(root)

    def test_io_probe_ignores_a_stale_inventory_from_another_checkout(self):
        path = Path(__file__).with_name("check-native-io-fault.py")
        spec = importlib.util.spec_from_file_location("io_probe", path)
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "schema-owner"
            target = root / "target/llvm-cov-target"
            target.mkdir(parents=True)
            profiles = target / "profiles"
            (target / "schema-owner-profraw-list").write_text(str(profiles / "run.profraw") + "\n")
            (target / "tinystoragedrivers-profraw-list").write_text(str(target / "stale/run.profraw"))

            def reached_native(*args, **kwargs):
                self.assertEqual(kwargs["env"]["LLVM_PROFILE_FILE"], str(profiles / "check-%p-%28m.profraw"))
                raise ReachedNativeExecution()

            with patch.object(probe, "ROOT", root), patch.object(probe, "capture_provenance"), \
                    patch.object(sys, "argv", ["probe", "--binary", str(root / "fixture")]), \
                    patch.object(probe.subprocess, "check_output", return_value="LLVM_PROFILE_FILE='/unused/check-%p-%28m.profraw'\n"), \
                    patch.object(probe.subprocess, "run", side_effect=reached_native) as native:
                with self.assertRaises(ReachedNativeExecution):
                    probe.main()
                self.assertEqual(native.call_count, 1)


if __name__ == "__main__":
    unittest.main()
