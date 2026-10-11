"""Finite diagnostic parsing and isolation; these are not native ACL proofs."""
import importlib.util
import json
from pathlib import Path, PureWindowsPath
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
spec = importlib.util.spec_from_file_location("user_denial", Path(__file__).with_name("check-native-user-denial.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def observed():
    return {"segments": [
        {"ordinal": ordinal, "role": role, "status": "Observed", "directory": True,
         "reparse": False, "owner_class": "System", "owner_trusted": True,
         "dacl": "Present", "dacl_protected": False,
         "aces": [{"allowed": True, "inherit_only": False, "effective_allowed": True,
                   "identity_class": "BuiltinUsers", "trusted": False, "mutation_mask": 2}]}
        for ordinal, role in enumerate(["Volume", "Outer", "Private"])]}


class FiniteAclDiagnostics(unittest.TestCase):
    def test_effective_foreign_mutation_is_preserved_without_identity_text(self):
        data = observed()
        result = probe.parse_acl_diagnostics(json.dumps(data))
        self.assertEqual(result, data)
        ace = result["segments"][0]["aces"][0]
        self.assertTrue(ace["effective_allowed"])
        self.assertFalse(ace["trusted"])
        self.assertEqual(ace["mutation_mask"], 2)

    def test_inherit_only_and_trusted_aces_remain_distinguishable(self):
        data = observed()
        inherited = data["segments"][0]["aces"][0]
        inherited.update(inherit_only=True, effective_allowed=False)
        trusted = data["segments"][1]["aces"][0]
        trusted.update(identity_class="TrustedInstaller", trusted=True,
                       mutation_mask=probe.MUTATION_MASK)
        self.assertEqual(probe.parse_acl_diagnostics(json.dumps(data)), data)

    def test_missing_and_unreadable_segments_have_only_finite_fields(self):
        data = {"segments": [{"ordinal": n, "role": role, "status": "Missing" if n == 2 else "ReadFailed"}
                             for n, role in enumerate(["Volume", "Outer", "Private"])]}
        self.assertEqual(probe.parse_acl_diagnostics(json.dumps(data)), data)

    def test_raw_fields_unknown_classes_and_inconsistent_effective_flags_are_rejected(self):
        for field, value in [("sid", "raw identity"), ("path", "private path"), ("sddl", "raw descriptor")]:
            with self.subTest(field=field):
                data = observed()
                data["segments"][0][field] = value
                with self.assertRaises(ValueError):
                    probe.parse_acl_diagnostics(json.dumps(data))
        for key, value in [("identity_class", "Unknown"), ("effective_allowed", False),
                           ("mutation_mask", 1), ("mutation_mask", -1), ("mutation_mask", True)]:
            with self.subTest(key=key, value=value):
                data = observed()
                data["segments"][0]["aces"][0][key] = value
                with self.assertRaises(ValueError):
                    probe.parse_acl_diagnostics(json.dumps(data))

    def test_malformed_oversized_and_extra_native_output_is_rejected(self):
        bad = ["not json", "[]", json.dumps({"segments": [], "raw": "native text"}), " " * 131073]
        data = observed()
        data["segments"][0]["aces"] *= 257
        bad.append(json.dumps(data))
        data = observed()
        data["segments"][0]["ordinal"] = True
        bad.append(json.dumps(data))
        data = observed()
        data["segments"][1]["dacl"] = "Empty"
        bad.append(json.dumps(data))
        for text in bad:
            with self.subTest(text_length=len(text)), self.assertRaises(ValueError):
                probe.parse_acl_diagnostics(text)

    def test_phase_classification_never_returns_arbitrary_stderr(self):
        self.assertEqual(probe.fixture_failure_phase("path/token namespace permits foreign mutation"),
                         "NamespaceForeignMutation")
        self.assertEqual(probe.fixture_failure_phase("arbitrary native text"), "OtherSetupFailure")

    def test_paths_are_environment_data_and_only_finite_diagnostics_are_saved(self):
        outer = PureWindowsPath("C:/fixture'$(source)/outer")
        private = outer / "private"
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            result = subprocess.CompletedProcess([], 0, json.dumps(observed()), "raw error text")
            with patch.object(probe.subprocess, "run", return_value=result) as run:
                probe.capture_windows_setup_diagnostics(outer, private,
                                                       "namespace permits foreign mutation", evidence)
            args, kwargs = run.call_args
            self.assertEqual(args[0][-1], probe.ACL_DIAGNOSTIC_SCRIPT)
            self.assertNotIn(str(outer), args[0][-1])
            self.assertEqual(json.loads(kwargs["env"]["TSD_ACL_DIAGNOSTIC_PATHS"]),
                             [outer.anchor, str(outer), str(private)])
            self.assertEqual(kwargs["timeout"], 30)
            saved = (evidence / "fixture-acl-diagnostics.json").read_text()
            self.assertNotIn("raw error text", saved)
            self.assertNotIn(str(outer), saved)
            self.assertEqual(json.loads(saved)["diagnostic_status"], "Observed")
            self.assertEqual(len(list(evidence.iterdir())), 1)

    def test_failed_or_rejected_subprocess_output_is_not_retained(self):
        results = [subprocess.CompletedProcess([], 1, "raw identity", "raw path"),
                   subprocess.CompletedProcess([], 0, '{"raw":"raw identity"}', "raw path")]
        for result, expected in zip(results, ["ProcessFailed", "ParserRejected"]):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                evidence = Path(directory)
                with patch.object(probe.subprocess, "run", return_value=result):
                    probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                           PureWindowsPath("C:/outer/private"), "raw path", evidence)
                saved = json.loads((evidence / "fixture-acl-diagnostics.json").read_text())
                self.assertEqual(saved, {"source_phase": "OtherSetupFailure", "diagnostic_status": expected})

    def test_subprocess_timeout_and_missing_powershell_remain_finite(self):
        for failure in [subprocess.TimeoutExpired("powershell", 30, output="raw identity"),
                        OSError("raw native path")]:
            with self.subTest(kind=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                with patch.object(probe.subprocess, "run", side_effect=failure):
                    probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                           PureWindowsPath("C:/outer/private"), "", Path(directory))
                saved = json.loads((Path(directory) / "fixture-acl-diagnostics.json").read_text())
                self.assertEqual(saved["diagnostic_status"], "ProcessFailed")
                self.assertEqual(set(saved), {"source_phase", "diagnostic_status"})


if __name__ == "__main__":
    unittest.main()
