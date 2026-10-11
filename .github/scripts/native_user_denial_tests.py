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
    def test_nonzero_process_retains_only_bounded_exit_and_finite_context(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            result = subprocess.CompletedProcess([], 21, "PRIVATE_SENTINEL_STDOUT",
                                                 "PRIVATE_SENTINEL_STDERR")
            with patch.object(probe.shutil, "which", return_value="PRIVATE_SENTINEL_EXECUTABLE"), \
                    patch.object(probe.subprocess, "run", return_value=result):
                probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                       PureWindowsPath("C:/outer/private"), "", evidence)
            record = json.loads((evidence / "fixture-acl-diagnostics.json").read_text())
            self.assertEqual(record, {"source_phase": "OtherSetupFailure", "diagnostic_version": 1,
                                      "executable_resolution": "Resolved", "diagnostic_status": "ProcessFailed",
                                      "return_code": 21, "failure_stage": "Unknown", "error_category": "Unknown"})

    def test_launch_failure_is_distinct_from_timeout_without_exception_payload(self):
        cases = [(FileNotFoundError(2, "PRIVATE_SENTINEL_MESSAGE", "PRIVATE_SENTINEL_PATH"),
                  "LaunchFailed", {"launch_error_class": "FileNotFound", "native_os_error": 2}),
                 (subprocess.TimeoutExpired("PRIVATE_SENTINEL_COMMAND", 30,
                                            output="PRIVATE_SENTINEL_OUTPUT", stderr="PRIVATE_SENTINEL_ERROR"),
                  "TimedOut", {"timeout_seconds": 30})]
        for failure, status, fields in cases:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                with patch.object(probe.shutil, "which", return_value=None), \
                        patch.object(probe.subprocess, "run", side_effect=failure):
                    probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                           PureWindowsPath("C:/outer/private"), "", Path(directory))
                text = (Path(directory) / "fixture-acl-diagnostics.json").read_text()
                self.assertNotIn("PRIVATE_SENTINEL", text)
                self.assertEqual(json.loads(text), {"source_phase": "OtherSetupFailure", "diagnostic_version": 1,
                                                    "executable_resolution": "Missing", "diagnostic_status": status,
                                                    **fields})

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
            with patch.object(probe.subprocess, "run", return_value=result) as run, patch("builtins.print") as output:
                probe.capture_windows_setup_diagnostics(outer, private,
                                                       "namespace permits foreign mutation", evidence)
                output.assert_not_called()
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
                self.assertEqual(saved["source_phase"], "OtherSetupFailure")
                self.assertEqual(saved["diagnostic_status"], expected)
                self.assertNotIn("raw", json.dumps(saved))
                probe.validate_diagnostic_record(saved)

    def test_subprocess_timeout_and_missing_powershell_remain_finite(self):
        for failure in [subprocess.TimeoutExpired("powershell", 30, output="raw identity"),
                        OSError("raw native path")]:
            with self.subTest(kind=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                with patch.object(probe.subprocess, "run", side_effect=failure):
                    probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                           PureWindowsPath("C:/outer/private"), "", Path(directory))
                saved = json.loads((Path(directory) / "fixture-acl-diagnostics.json").read_text())
                self.assertEqual(saved["diagnostic_status"], "TimedOut" if isinstance(failure, subprocess.TimeoutExpired) else "LaunchFailed")
                self.assertNotIn("raw", json.dumps(saved))
                probe.validate_diagnostic_record(saved)

    def test_script_envelope_has_exact_fields_and_finite_stage_category(self):
        self.assertEqual(probe.parse_script_failure('{"status":"ScriptFailed","stage":"Paths","category":"InvalidArgument"}'),
                         ("Paths", "InvalidArgument"))
        for value in [{"status": "ScriptFailed", "stage": "Unknown", "category": "Other"},
                      {"status": "ScriptFailed", "stage": "Paths", "category": "PRIVATE_SENTINEL"},
                      {"status": "ScriptFailed", "stage": "Paths", "category": "Other", "message": "PRIVATE_SENTINEL"}]:
            with self.subTest(keys=list(value)), self.assertRaises(ValueError):
                probe.parse_script_failure(json.dumps(value))
        for text in ["PRIVATE_SENTINEL", " " * 131073,
                     '{"status":"ScriptFailed","stage":"Paths","category":"Other"}\nPRIVATE_SENTINEL']:
            with self.subTest(length=len(text)), self.assertRaises(ValueError):
                probe.parse_script_failure(text)

    def test_parser_marker_classification_is_exact_and_context_only(self):
        self.assertEqual(probe.parser_error_category("PRIVATE_SENTINEL\n+ CategoryInfo : ParserError: (:) []"),
                         "ParserError")
        for text in ["PRIVATE_SENTINEL ParserError", "message: CategoryInfo : ParserError",
                     "CategoryInfo : ParserErrorUnknown", "FullyQualifiedErrorId : ParserError"]:
            self.assertEqual(probe.parser_error_category(text), "Unknown")

    def test_every_outcome_is_an_exact_key_union_and_rejects_boolean_integer_bounds(self):
        common = {"source_phase": "OtherSetupFailure", "diagnostic_version": 1,
                  "executable_resolution": "Resolved"}
        cases = [{"diagnostic_status": "Observed", **observed()},
                 {"diagnostic_status": "LaunchFailed", "launch_error_class": "PermissionDenied", "native_os_error": 5},
                 {"diagnostic_status": "TimedOut", "timeout_seconds": 30},
                 {"diagnostic_status": "ProcessFailed", "return_code": 1, "failure_stage": "Identity", "error_category": "Other"},
                 {"diagnostic_status": "ParserRejected"}]
        for case in cases:
            record = {**common, **case}
            self.assertEqual(probe.validate_diagnostic_record(record), record)
            for field in ["path", "sid", "message", "stderr", "command"]:
                with self.subTest(field=field), self.assertRaises(ValueError):
                    probe.validate_diagnostic_record({**record, field: "PRIVATE_SENTINEL"})
            with self.assertRaises(ValueError):
                probe.validate_diagnostic_record({**record, "diagnostic_version": True})
        for value in [True, -1, 2**32]:
            with self.subTest(os_number=value), self.assertRaises(ValueError):
                probe.validate_diagnostic_record({**common, **cases[1], "native_os_error": value})
        for value in [True, 0, -(2**31) - 1, 2**32]:
            with self.subTest(return_code=value), self.assertRaises(ValueError):
                probe.validate_diagnostic_record({**common, **cases[3], "return_code": value})
        for value in [-(2**31), 2**32 - 1]:
            probe.validate_diagnostic_record({**common, **cases[3], "return_code": value})
        for key in ["source_phase", "executable_resolution", "failure_stage", "error_category"]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                probe.validate_diagnostic_record({**common, **cases[3], key: "PRIVATE_SENTINEL"})

    def test_nonzero_failure_envelope_and_parser_stderr_do_not_retain_raw_streams(self):
        inputs = [('{"status":"ScriptFailed","stage":"Serialize","category":"InvalidOperation"}',
                   "PRIVATE_SENTINEL", "Serialize", "InvalidOperation"),
                  ("PRIVATE_SENTINEL", "+ CategoryInfo : ParserError: PRIVATE_SENTINEL", "Unknown", "ParserError"),
                  ('{"status":"ScriptFailed","stage":"Paths","category":"Other","sid":"PRIVATE_SENTINEL"}',
                   "PRIVATE_SENTINEL", "Unknown", "Unknown")]
        for stdout, stderr, stage, category in inputs:
            with self.subTest(stage=stage, category=category), tempfile.TemporaryDirectory() as directory:
                result = subprocess.CompletedProcess([], 1, stdout, stderr)
                with patch.object(probe.shutil, "which", return_value="PRIVATE_SENTINEL"), \
                        patch.object(probe.subprocess, "run", return_value=result):
                    record = probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                                     PureWindowsPath("C:/outer/private"), "", Path(directory))
                self.assertEqual(record["failure_stage"], stage)
                self.assertEqual(record["error_category"], category)
                self.assertNotIn("PRIVATE_SENTINEL", (Path(directory) / "fixture-acl-diagnostics.json").read_text())

    def test_resolution_failure_and_resolved_but_failed_launch_are_distinct(self):
        for discovery, launch, resolution, kind in [
            (OSError("PRIVATE_SENTINEL"), PermissionError(13, "PRIVATE_SENTINEL"), "ResolutionFailed", "PermissionDenied"),
            ("PRIVATE_SENTINEL", FileNotFoundError(2, "PRIVATE_SENTINEL"), "Resolved", "FileNotFound")]:
            with self.subTest(resolution=resolution), tempfile.TemporaryDirectory() as directory:
                mock = patch.object(probe.shutil, "which", side_effect=discovery) if isinstance(discovery, OSError) else patch.object(probe.shutil, "which", return_value=discovery)
                with mock, patch.object(probe.subprocess, "run", side_effect=launch):
                    record = probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                                     PureWindowsPath("C:/outer/private"), "", Path(directory))
                self.assertEqual(record["executable_resolution"], resolution)
                self.assertEqual(record["launch_error_class"], kind)
                self.assertNotIn("PRIVATE_SENTINEL", json.dumps(record))

    def test_invalid_process_metadata_and_os_numbers_are_not_retained(self):
        for number in [True, -(2**31) - 1, 2**32]:
            with self.subTest(number=number), tempfile.TemporaryDirectory() as directory:
                with patch.object(probe.subprocess, "run", return_value=subprocess.CompletedProcess([], number, "PRIVATE_SENTINEL", "PRIVATE_SENTINEL")):
                    record = probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                                     PureWindowsPath("C:/outer/private"), "", Path(directory))
                self.assertEqual(record["diagnostic_status"], "ParserRejected")
                self.assertNotIn("return_code", record)
        for number in [True, -1, 2**32]:
            with self.subTest(os_number=number), tempfile.TemporaryDirectory() as directory:
                error = OSError("PRIVATE_SENTINEL")
                error.winerror = number
                with patch.object(probe.subprocess, "run", side_effect=error):
                    record = probe.capture_windows_setup_diagnostics(PureWindowsPath("C:/outer"),
                                                                     PureWindowsPath("C:/outer/private"), "", Path(directory))
                self.assertNotIn("native_os_error", record)

    def test_import_does_not_execute_cargo_launcher_or_account_fixture(self):
        isolated = importlib.util.module_from_spec(spec)
        with patch.object(subprocess, "run", side_effect=AssertionError("import executed a subprocess")):
            spec.loader.exec_module(isolated)


if __name__ == "__main__":
    unittest.main()
