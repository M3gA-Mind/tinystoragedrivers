"""Finite diagnostic parsing and isolation; these are not native ACL proofs."""
import ast
import importlib.util
import json
import os
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


def print_finite_failure(record):
    """Retain only validated diagnostics when a native assertion fails."""
    probe.validate_diagnostic_record(record)
    print(json.dumps(record, sort_keys=True))


DISPOSAL_EXCEPTION_CLASSES = {"ObjectDisposed", "GetValueInvocation", "MethodInvocation",
                              "TargetInvocation", "Runtime", "Other"}
DISPOSAL_STAGES = {"Start", "Lifecycle", "CaughtAssertion", "Getter", "MethodProbe", "DisposedAssertion", "Complete"}


def parse_disposal_diagnostic(text):
    """Validate only static lifecycle metadata; discard all native payloads."""
    if len(text) > 8192:
        raise ValueError("invalid finite disposal diagnostic")
    value = json.loads(text)
    fields = {"status", "stage", "phase", "caught", "getter_status", "getter_null",
              "getter_exception_classes", "method_status", "method_exception_classes", "exception_classes"}
    if (type(value) is not dict or set(value) != fields
            or value["status"] not in {"Passed", "Failed"} or value["stage"] not in DISPOSAL_STAGES
            or value["phase"] not in {"Construction", "Probe", "Cleanup"}
            or type(value["caught"]) is not bool or type(value["getter_null"]) is not bool
            or value["getter_status"] not in {"NotRun", "Returned", "TypedCaught", "OtherCaught"}
            or value["method_status"] not in {"NotRun", "Returned", "Threw"}):
        raise ValueError("invalid finite disposal diagnostic")
    for field in ["getter_exception_classes", "method_exception_classes", "exception_classes"]:
        chain = value[field]
        if (type(chain) is not list or len(chain) > 4
                or any(type(item) is not str or item not in DISPOSAL_EXCEPTION_CLASSES for item in chain)):
            raise ValueError("invalid finite disposal diagnostic")
    return value


def method_proves_disposal(status, chain):
    """Require the proven disposed-method exception, not any failure or getter."""
    wrappers = {"GetValueInvocation", "MethodInvocation", "TargetInvocation", "Runtime"}
    return (status == "Threw" and type(chain) is list and 1 <= len(chain) <= 4
            and chain[-1] == "ObjectDisposed" and all(item in wrappers for item in chain[:-1]))


DISPOSAL_EXCEPTION_SCRIPT = r"""function FiniteExceptionChain($exception) {
 $classes=@()
 for($depth=0;$depth -lt 4 -and $null -ne $exception;$depth++) {
  $class=switch($exception.GetType().FullName) {
   'System.ObjectDisposedException' {'ObjectDisposed'}
   'System.Management.Automation.GetValueInvocationException' {'GetValueInvocation'}
   'System.Management.Automation.MethodInvocationException' {'MethodInvocation'}
   'System.Reflection.TargetInvocationException' {'TargetInvocation'}
   'System.Management.Automation.RuntimeException' {'Runtime'}
   default {'Other'}
  }
  $classes+=$class
  $exception=$exception.InnerException
 }
 return ,$classes
}
"""


def parse_engine_metadata(text):
    """Accept bounded engine components and module-path booleans only."""
    if len(text) > 8192:
        raise ValueError("invalid finite engine metadata")
    record = json.loads(text)
    if (type(record) is not dict or set(record) != {"edition", "major", "minor", "build", "revision", "module_path_present", "child_default_included"}
            or record["edition"] not in {"Desktop", "Core", "Other"}
            or any(type(record[key]) is not int or not -1 <= record[key] <= 65535 for key in ["major", "minor", "build", "revision"])
            or any(type(record[key]) is not bool for key in ["module_path_present", "child_default_included"])):
        raise ValueError("invalid finite engine metadata")
    return record


class FiniteDisposalDiagnostics(unittest.TestCase):
    def test_actual_null_getter_red_is_disposed_only_by_known_method_exception_chain(self):
        self.assertTrue(method_proves_disposal("Threw", ["MethodInvocation", "ObjectDisposed"]))
        for status, chain in [("Returned", []), ("Threw", ["Other"]),
                              ("Threw", ["Other", "ObjectDisposed"]),
                              ("Threw", ["MethodInvocation"]),
                              ("Threw", ["ObjectDisposed", "Runtime"]),
                              ("Threw", ["Runtime"] * 4 + ["ObjectDisposed"])]:
            with self.subTest(status=status, chain=chain):
                self.assertFalse(method_proves_disposal(status, chain))

    def test_engine_metadata_rejects_paths_unknown_editions_and_nonfinite_components(self):
        record = {"edition": "Desktop", "major": 5, "minor": 1, "build": 0, "revision": -1,
                  "module_path_present": True, "child_default_included": False}
        self.assertEqual(parse_engine_metadata(json.dumps(record)), record)
        for key, value in [("edition", "PRIVATE_SENTINEL"), ("major", True), ("build", 65536),
                           ("module_path_present", "PRIVATE_SENTINEL"), ("path", "PRIVATE_SENTINEL")]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                parse_engine_metadata(json.dumps({**record, key: value}))

    def test_strict_static_envelope_rejects_payloads_types_and_unbounded_chains(self):
        record = {"status": "Failed", "stage": "Getter", "phase": "Probe", "caught": True,
                  "getter_status": "OtherCaught", "getter_null": False,
                  "getter_exception_classes": ["GetValueInvocation", "ObjectDisposed"],
                  "method_status": "Threw", "method_exception_classes": ["MethodInvocation", "ObjectDisposed"],
                  "exception_classes": ["GetValueInvocation", "ObjectDisposed"]}
        self.assertEqual(parse_disposal_diagnostic(json.dumps(record)), record)
        for field, value in [("stage", "PRIVATE_SENTINEL"), ("phase", 1), ("caught", 1),
                             ("getter_null", None), ("getter_status", "PRIVATE_SENTINEL"),
                             ("method_status", False), ("exception_classes", ["Other"] * 5),
                             ("getter_exception_classes", [False]),
                             ("method_exception_classes", ["System.PRIVATE_SENTINEL"]),
                             ("password", "PRIVATE_SENTINEL"), ("path", "PRIVATE_SENTINEL")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                parse_disposal_diagnostic(json.dumps({**record, field: value}))
        for text in ["PRIVATE_SENTINEL", "[]", " " * 8193]:
            with self.assertRaises(ValueError):
                parse_disposal_diagnostic(text)


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

    def test_failure_print_validates_before_disclosing_any_record(self):
        record = {"source_phase": "OtherSetupFailure", "diagnostic_version": 1,
                  "executable_resolution": "Resolved", "diagnostic_status": "Observed",
                  **observed()}
        record["segments"][0] = {"ordinal": 0, "role": "Volume", "status": "ReadFailed",
                                  "read_stage": "Acl", "error_category": "ObjectNotFound",
                                  "exception_class": "CommandNotFound",
                                  "inner_exception_classes": ["Other"]}
        with patch("builtins.print") as output:
            print_finite_failure(record)
            self.assertEqual(json.loads(output.call_args.args[0]), record)
        for field in ["path", "sid", "sddl", "message"]:
            with self.subTest(field=field), patch("builtins.print") as output:
                with self.assertRaises(ValueError):
                    print_finite_failure({**record, field: "PRIVATE_SECRET_SENTINEL"})
                output.assert_not_called()

    def test_unreadable_segment_requires_bounded_exception_classification(self):
        data = observed()
        data["segments"][0] = {"ordinal": 0, "role": "Volume", "status": "ReadFailed",
                               "read_stage": "Acl", "error_category": "ObjectNotFound"}
        with self.assertRaises(ValueError):
            probe.parse_acl_diagnostics(json.dumps(data))

    def test_exception_classes_are_exact_finite_and_wrapping_is_bounded(self):
        classes = ["CommandNotFound", "ItemNotFound", "DriveNotFound", "ProviderNotFound",
                   "ActionPreferenceStop", "UnauthorizedAccess", "ParameterBinding", "Other"]
        for name in classes:
            data = observed()
            segment = {"ordinal": 0, "role": "Volume", "status": "ReadFailed",
                       "read_stage": "Acl", "error_category": "ObjectNotFound",
                       "exception_class": name, "inner_exception_classes": ["Other", name]}
            data["segments"][0] = segment
            self.assertEqual(probe.parse_acl_diagnostics(json.dumps(data)), data)
            for key, value in [("exception_class", True), ("exception_class", 1),
                               ("exception_class", "System.PRIVATE_PATH_SENTINEL"),
                               ("exception_class", ["Other"]),
                               ("exception_class", {"sid": "S-1-PRIVATE_SID"}),
                               ("inner_exception_classes", ["Other"] * 3),
                               ("inner_exception_classes", [False]),
                               ("inner_exception_classes", "Other"),
                               ("inner_exception_classes", ["S-1-PRIVATE_SID"]),
                               ("message", "PRIVATE_SECRET_SENTINEL"),
                               ("sddl", "PRIVATE_SDDL_SENTINEL"),
                               ("path", "PRIVATE_PATH_SENTINEL")]:
                with self.subTest(name=name, key=key), self.assertRaises(ValueError):
                    probe.parse_acl_diagnostics(json.dumps({"segments": [
                        {**segment, key: value}, *data["segments"][1:]]}))

    def test_unreadable_segment_requires_operation_and_category(self):
        data = observed()
        data["segments"][0] = {"ordinal": 0, "role": "Volume", "status": "ReadFailed"}
        with self.assertRaises(ValueError):
            probe.parse_acl_diagnostics(json.dumps(data))

    def test_unreadable_operation_and_category_are_finite_and_exact(self):
        stages = ["Existence", "Item", "Acl", "Descriptor", "Owner", "DaclLimit",
                  "AceType", "AceFlags", "AceIdentity", "AceMask", "AceClassification",
                  "DaclShape", "ItemMetadata", "OwnerClassification", "Record"]
        for stage in stages:
            data = observed()
            segment = {"ordinal": 0, "role": "Volume", "status": "ReadFailed",
                       "read_stage": stage, "error_category": "PermissionDenied",
                       "exception_class": "Other", "inner_exception_classes": []}
            data["segments"][0] = segment
            self.assertEqual(probe.parse_acl_diagnostics(json.dumps(data)), data)
            for key, value in [("read_stage", True), ("read_stage", 1),
                               ("read_stage", "PRIVATE_PATH_SENTINEL"),
                               ("error_category", False), ("error_category", "S-1-PRIVATE_SID"),
                               ("path", "PRIVATE_PATH_SENTINEL"), ("sddl", "PRIVATE_SDDL_SENTINEL"),
                               ("message", "PRIVATE_SECRET_SENTINEL")]:
                with self.subTest(stage=stage, key=key), self.assertRaises(ValueError):
                    probe.parse_acl_diagnostics(json.dumps({"segments": [
                        {**segment, key: value}, *data["segments"][1:]]}))

    def test_missing_and_unreadable_segments_have_only_finite_fields(self):
        data = {"segments": [{"ordinal": n, "role": role, "status": "Missing" if n == 2 else "ReadFailed"}
                             for n, role in enumerate(["Volume", "Outer", "Private"])]}
        for segment in data["segments"][:2]:
            segment.update(read_stage="Acl", error_category="PermissionDenied",
                           exception_class="Other", inner_exception_classes=[])
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


class NativeFixtureCommand(unittest.TestCase):
    def test_production_probe_explicitly_enables_file_without_default_engines(self):
        command = probe.fixture_test_command()
        self.assertIn("--no-default-features", command)
        feature_index = command.index("--features")
        self.assertEqual(command[feature_index + 1], "file")
        self.assertEqual(command[command.index("-p") + 1], "tinystoragedrivers-secrets")
        self.assertEqual(command[command.index("--lib") + 1], probe.TEST)


@unittest.skipUnless(os.name == "nt", "requires the actual native Windows PowerShell launcher")
class NativeWindowsPathDecoder(unittest.TestCase):
    def capture_paths(self, paths, outer, private, evidence):
        """Replace only environment data, leaving the real helper launcher intact."""
        run = probe.subprocess.run

        def native_run(args, **kwargs):
            self.assertTrue(args == ["powershell", "-NoProfile", "-NonInteractive", "-Command",
                                     probe.ACL_DIAGNOSTIC_SCRIPT], "native launcher changed")
            self.assertTrue(kwargs["timeout"] == 30, "native launcher timeout changed")
            kwargs["env"] = {**kwargs["env"], "TSD_ACL_DIAGNOSTIC_PATHS": paths}
            result = run(args, **kwargs)
            # Assertions expose only fixed messages, never the captured streams.
            self.assertTrue(not result.stderr.strip(), "native decoder emitted unexpected stderr")
            self.assertTrue("TSD_PRIVATE_SENTINEL" not in result.stdout,
                            "native decoder disclosed input data")
            self.assertTrue("S-1-" not in result.stdout, "native decoder disclosed an identity")
            return result

        with patch.object(probe.subprocess, "run", side_effect=native_run):
            record = probe.capture_windows_setup_diagnostics(outer, private, "", evidence)
        saved = (evidence / "fixture-acl-diagnostics.json").read_text()
        self.assertTrue("TSD_PRIVATE_SENTINEL" not in saved, "saved diagnostic disclosed input data")
        probe.validate_diagnostic_record(record)
        return record

    def test_three_strings_preserve_literal_paths_and_ordinal_order(self):
        with tempfile.TemporaryDirectory(prefix="tsd-decoder-") as name:
            outer = Path(name)
            private = outer / "TSD_PRIVATE_SENTINEL ' [literal] $(Write-Output LEAK)"
            private.mkdir()
            file = private / "file ' [literal] $(Write-Output LEAK)"
            file.write_text("public decoder fixture\n")
            missing = private / "absent ' [literal] $(Write-Output LEAK)"
            record = self.capture_paths(json.dumps([str(file), str(missing), str(private)]),
                                        outer, private, outer)
            try:
                self.assertEqual(record["diagnostic_status"], "Observed")
                segments = record["segments"]
                self.assertEqual([segment["status"] for segment in segments],
                                 ["Observed", "Missing", "Observed"])
                self.assertEqual([segment["ordinal"] for segment in segments], [0, 1, 2])
                self.assertEqual([segment["role"] for segment in segments],
                                 ["Volume", "Outer", "Private"])
                self.assertFalse(segments[0]["directory"])
                self.assertTrue(segments[2]["directory"])
                # Both filesystem overloads must yield owner and discretionary ACL
                # metadata through the actual launcher, without the Get-Acl cmdlet.
                for segment in [segments[0], segments[2]]:
                    self.assertFalse(segment["reparse"])
                    self.assertIn(segment["owner_class"],
                                  {"Current", "System", "Admins", "TrustedInstaller"})
                    self.assertTrue(segment["owner_trusted"])
                    self.assertEqual(segment["dacl"], "Present")
                    self.assertTrue(segment["aces"])
                self.assertEqual(segments[0]["owner_class"], segments[2]["owner_class"])
            except AssertionError:
                print_finite_failure(record)
                raise

    def test_nonflat_nonstring_wrong_count_and_malformed_json_fail_before_acl_reads(self):
        sentinel = "TSD_PRIVATE_SENTINEL ' [literal] $(Write-Output LEAK)"
        values = [None, sentinel, {"path": sentinel}, [], [sentinel], [sentinel] * 2,
                  [sentinel] * 4, [[sentinel] * 3], [[sentinel], sentinel, sentinel],
                  [1, sentinel, sentinel], [None, sentinel, sentinel],
                  [{"path": sentinel}, sentinel, sentinel], ["", sentinel, sentinel]]
        texts = [json.dumps(value) for value in values] + ['["TSD_PRIVATE_SENTINEL",', "not json"]
        with tempfile.TemporaryDirectory(prefix="tsd-decoder-") as name:
            outer = Path(name)
            for case, paths in enumerate(texts):
                with self.subTest(case=case):
                    record = self.capture_paths(paths, outer, outer / "private", outer)
                    self.assertEqual(record, {"source_phase": "OtherSetupFailure", "diagnostic_version": 1,
                                              "executable_resolution": "Resolved", "diagnostic_status": "ProcessFailed",
                                              "return_code": 21, "failure_stage": "Paths", "error_category": "InvalidArgument"})


class OuterFixtureReadback(unittest.TestCase):
    def test_reconstruction_paths_remain_environment_json_and_native_failure_is_static(self):
        outer = PureWindowsPath("C:/tsd-denial-PRIVATE_SENTINEL'$(source)")
        outcomes = [subprocess.CompletedProcess([], 1, "PRIVATE_SENTINEL", "PRIVATE_SENTINEL"),
                    subprocess.CompletedProcess([], 0, "OUTER_ACL_RECONSTRUCTED", "PRIVATE_SENTINEL"),
                    subprocess.CompletedProcess([], 0, "PRIVATE_SENTINEL", ""),
                    OSError("PRIVATE_SENTINEL"),
                    subprocess.TimeoutExpired("PRIVATE_SENTINEL", 30, output="PRIVATE_SENTINEL")]
        for case, outcome in enumerate(outcomes):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as name:
                mocked = patch.object(probe.subprocess, "run", side_effect=outcome) if isinstance(outcome, Exception) else patch.object(probe.subprocess, "run", return_value=outcome)
                with mocked as run, self.assertRaisesRegex(RuntimeError, "^disposable outer ACL reconstruction failed$"):
                    probe.configure_windows_outer(outer, Path(name))
                args, kwargs = run.call_args
                self.assertEqual(args[0][-1], probe.OUTER_ACL_SCRIPT)
                self.assertNotIn(str(outer), args[0][-1])
                self.assertEqual(json.loads(kwargs["env"]["TSD_OUTER_FIXTURE_PATH"]), str(outer))
                self.assertEqual(kwargs["timeout"], 30)
                self.assertEqual(list(Path(name).iterdir()), [])

    def test_successful_setter_still_requires_actual_readback(self):
        result = subprocess.CompletedProcess([], 0, "OUTER_ACL_RECONSTRUCTED\n", "")
        with tempfile.TemporaryDirectory() as name:
            outer = PureWindowsPath("C:/tsd-denial-owned")
            with patch.object(probe.subprocess, "run", return_value=result), \
                    patch.object(probe, "capture_windows_setup_diagnostics", return_value={"diagnostic_status": "ParserRejected"}) as readback, \
                    self.assertRaisesRegex(RuntimeError, "^disposable outer ACL readback failed$"):
                probe.configure_windows_outer(outer, Path(name))
            readback.assert_called_once_with(outer, outer / "private", "", Path(name))

    def test_requires_exact_protected_current_owned_trusted_rules_and_foreign_read_only(self):
        segment = observed()["segments"][1]
        segment.update(owner_class="Current", owner_trusted=True, dacl_protected=True)
        segment["aces"] = [
            {"allowed": True, "inherit_only": False, "effective_allowed": True,
             "identity_class": name, "trusted": name != "Everyone",
             "mutation_mask": 0 if name == "Everyone" else 0xD0152}
            for name in ["Current", "System", "Admins", "Everyone"]]
        probe.validate_windows_outer_acl(segment)
        for field, value in [("owner_class", "Admins"), ("dacl_protected", False),
                             ("reparse", True), ("directory", False)]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                probe.validate_windows_outer_acl({**segment, field: value})
        for name in ["Other", "Everyone"]:
            invalid = {**segment, "aces": [*segment["aces"],
                       {**segment["aces"][-1], "identity_class": name, "mutation_mask": 2}]}
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                probe.validate_windows_outer_acl(invalid)
        with self.assertRaises(RuntimeError):
            probe.validate_windows_outer_acl({**segment, "aces": segment["aces"][:-1]})


class FiniteChildAndCleanupDiagnostics(unittest.TestCase):
    def test_child_schema_rejects_payloads_unknown_stages_chains_ordinals_and_exit_mismatch(self):
        record = {"status": "Failed", "stage": "PrivateReplace", "object_ordinal": 2,
                  "expected_exit": 1, "error_category": "InvalidOperation",
                  "exception_classes": ["MethodInvocation", "IOException"]}
        self.assertEqual(probe.parse_child_diagnostic(json.dumps(record)), record)
        for field, value in [("path", "PRIVATE_SENTINEL"), ("password", "PRIVATE_SENTINEL"),
                             ("stage", "PRIVATE_SENTINEL"), ("object_ordinal", True),
                             ("object_ordinal", 4), ("expected_exit", 19), ("expected_exit", 13),
                             ("error_category", "PRIVATE_SENTINEL"),
                             ("exception_classes", ["Other"] * 4),
                             ("exception_classes", ["System.PRIVATE_SENTINEL"]),
                             ("exception_classes", [False])]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                probe.parse_child_diagnostic(json.dumps({**record, field: value}))
        for text in ["PRIVATE_SENTINEL", "[]", " " * 8193]:
            with self.assertRaises(ValueError):
                probe.parse_child_diagnostic(text)

    def test_output_projection_keeps_only_recognized_metadata_and_original_success_marker(self):
        record = {"status": "Failed", "stage": "NonAdmin", "object_ordinal": None,
                  "expected_exit": 11, "error_category": "NotSpecified", "exception_classes": []}
        self.assertEqual(probe.classify_child_output(json.dumps(record), "", 11),
                         {"diagnostic_status": "Observed", **record})
        self.assertEqual(probe.classify_child_output(json.dumps(record), "PRIVATE_SENTINEL", 11),
                         {"diagnostic_status": "ParserRejected"})
        for text, stderr, code in [("PRIVATE_SENTINEL", "PRIVATE_SENTINEL", 1),
                                   ("ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED", "", 1)]:
            self.assertEqual(probe.classify_child_output(text, stderr, code), {"diagnostic_status": "ParserRejected"})
        self.assertEqual(probe.classify_child_output("ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED", "", 0),
                         {"diagnostic_status": "SuccessMarker"})
        startup = probe.classify_child_output("", "PRIVATE_SENTINEL RunningScriptsDisabled PSSecurityException", 1)
        self.assertTrue(startup["running_scripts_disabled"])
        self.assertTrue(startup["ps_security_exception"])
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(startup))

    def test_source_write_stages_and_argument_subtypes_are_finite(self):
        for stage, ordinal in [("PublicReplacementSourceWrite", None),
                               ("PrivateReplacementSourceWrite", 3), ("PublicReplace", None)]:
            for name in ["ArgumentNull", "ArgumentOutOfRange", "Argument", "NotSupported", "Security"]:
                record = {"status": "Failed", "stage": stage, "object_ordinal": ordinal,
                          "expected_exit": 1, "error_category": "InvalidArgument",
                          "exception_classes": ["MethodInvocation", name]}
                self.assertEqual(probe.parse_child_diagnostic(json.dumps(record)), record)

    def test_replace_comparison_schema_rejects_payloads_and_unbounded_values(self):
        record = {"candidate": "OrdinaryNull", "outcome": "Failed",
                  "exception_classes": ["MethodInvocation", "Argument"],
                  "target_original": True, "target_replaced": False,
                  "source_retained": True, "source_original": True, "backup_absent": True}
        self.assertEqual(probe.parse_replace_comparison(json.dumps(record)), record)
        for field, value in [("candidate", []), ("outcome", True), ("target_original", 1),
                             ("exception_classes", ["Other"] * 4),
                             ("exception_classes", ["PRIVATE_SENTINEL"]),
                             ("path", "PRIVATE_SENTINEL")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                probe.parse_replace_comparison(json.dumps({**record, field: value}))
        for stage in [[], {}, True]:
            invalid = {"status": "Failed", "stage": stage, "object_ordinal": None,
                       "expected_exit": 1, "error_category": "NotSpecified", "exception_classes": []}
            with self.assertRaises(ValueError):
                probe.parse_child_diagnostic(json.dumps(invalid))

    def test_source_write_is_outside_typed_private_target_denial(self):
        child = """$ErrorActionPreference='Stop'
$source=$control+'.replacement'
[IO.File]::WriteAllText($source, 'public-fixture-control')
[IO.File]::Replace($source,$control,$null)
foreach($f in @(FILES)) {
 try { $h=[IO.File]::OpenRead($f); exit 13 }
 catch [UnauthorizedAccessException] { }
 [IO.File]::WriteAllText($source, 'public-fixture-control')
 try { [IO.File]::Replace($source,$f,$null); exit 16 }
 catch [UnauthorizedAccessException] { }
}
"""
        actual = probe.instrument_windows_child(child)
        self.assertLess(actual.index("$stage='PublicReplacementSourceWrite'"),
                        actual.index("$stage='PublicReplace'"))
        write = actual.index("$stage='PrivateReplacementSourceWrite'")
        replace = actual.index("$stage='PrivateReplace'")
        self.assertLess(write, actual.index(" [IO.File]::WriteAllText($source,", write))
        self.assertLess(write, replace)
        self.assertIn("try { [IO.File]::Replace($source,$f,[System.Management.Automation.Language.NullString]::Value); $expectedExit=16;exit 16 }", actual)
        self.assertEqual(actual.count("catch [UnauthorizedAccessException] { }"), 2)
        for candidate in ["OrdinaryNull", "TrueNull"]:
            script = probe.windows_replace_comparison_script(candidate)
            self.assertNotIn("New-LocalUser", script)
            self.assertNotIn("Set-Acl", script)
            self.assertNotIn("WriteAllBytes", script)
        with self.assertRaises(ValueError):
            probe.windows_replace_comparison_script("PRIVATE_SENTINEL")

    def test_executed_child_passes_true_null_backup_to_public_and_private_replace(self):
        tree = ast.parse(Path(probe.__file__).read_text())
        main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main")
        templates = [node.value for node in ast.walk(main)
                     if isinstance(node, ast.Constant) and isinstance(node.value, str)
                     and "[IO.File]::Replace($source,$control,$null)" in node.value]
        self.assertEqual(len(templates), 1)
        raw = templates[0]
        actual = probe.instrument_windows_child(raw)
        true_null = "[System.Management.Automation.Language.NullString]::Value"
        for target in ["$control", "$f"]:
            with self.subTest(target=target):
                self.assertIn(f"[IO.File]::Replace($source,{target},{true_null})", actual)
                self.assertNotIn(f"[IO.File]::Replace($source,{target},$null)", actual)
                # The historical oversized launch control remains unchanged.
                self.assertIn(f"[IO.File]::Replace($source,{target},$null)", raw)
        self.assertIn("foreach($f in @(FILES))", actual)
        self.assertEqual(actual.count("catch [UnauthorizedAccessException] { }"), 4)
        self.assertIn("if(-not [IO.File]::Exists($source)){$expectedExit=17;exit 17}", actual)
        self.assertIn("ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED", actual)

    def test_cleanup_can_succeed_independently_of_child_failure_and_rejects_false_claims(self):
        record = {"status": "Failed", "phase": "Child", "oversized_outcome": "ParameterIncorrect",
                  "oversized_length": 3875, "short_length": 118, "short_within_bound": True,
                  "child_exit": 1, "cleanup_complete": True, "cleanup_created": True,
                  "removal_attempted": True, "removal_succeeded": True,
                  "disposal_attempted": True, "disposal_succeeded": True}
        self.assertEqual(probe.parse_launch_diagnostic(json.dumps(record)), record)
        for field, value in [("removal_attempted", False), ("disposal_attempted", False),
                             ("disposal_succeeded", False), ("cleanup_complete", False),
                             ("cleanup_created", False), ("removal_succeeded", "PRIVATE_SENTINEL")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                probe.parse_launch_diagnostic(json.dumps({**record, field: value}))


@unittest.skipUnless(os.name == "nt", "requires native finite child instrumentation, creates no account")
class NativeWindowsChildDiagnostics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        stamp = probe.datetime.datetime.now(probe.datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        cls.evidence = probe.ROOT / "target" / ("native-user-denial-child-diagnostics-" + stamp)
        cls.evidence.mkdir(parents=True)
        probe.capture_provenance(probe.ROOT, cls.evidence)

    def test_explicit_exit_and_unexpected_exception_produce_only_finite_stages(self):
        samples = [("NonAdmin", None, "exit 11", 11),
                   ("PrivateRead", 2, "$stage='PrivateRead';$ordinal=2;exit 13", 13),
                   ("PublicRead", None, "$stage='PublicRead';throw [IO.IOException]::new('PRIVATE_SENTINEL')", 1)]
        for stage, ordinal, body, code in samples:
            with self.subTest(stage=stage):
                script = probe.instrument_windows_child("$ErrorActionPreference='Stop'\n" + body + "\n")
                result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
                record = probe.classify_child_output(result.stdout, result.stderr, result.returncode)
                (self.evidence / (stage + '.json')).write_text(json.dumps(record, sort_keys=True) + '\n')
                self.assertTrue(record.get("diagnostic_status") == "Observed", "native child diagnostic rejected")
                self.assertTrue(record.get("stage") == stage and record.get("object_ordinal") == ordinal,
                                "native child diagnostic stage mismatch")
                self.assertTrue(result.returncode == code, "native child diagnostic changed exit code")
                self.assertTrue('PRIVATE_SENTINEL' not in json.dumps(record), "native child diagnostic disclosed payload")

    def test_owned_same_files_compare_backup_null_without_accounts(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source, target, backup = [root / part for part in ["source", "target", "backup"]]
            observations = {}
            for candidate in ["OrdinaryNull", "TrueNull"]:
                for path in [source, target, backup]:
                    path.unlink(missing_ok=True)
                source.write_bytes(bytes([11, 22, 33]))
                target.write_bytes(bytes([44, 55, 66]))
                identity = (source.stat().st_dev, source.stat().st_ino)
                result = subprocess.run(
                    ["powershell", "-NoProfile", "-Command", probe.windows_replace_comparison_script(candidate)],
                    env={**os.environ, "TSD_REPLACE_COMPARISON_ROOT": str(root)},
                    capture_output=True, text=True, timeout=30)
                try:
                    record = probe.parse_replace_comparison(result.stdout)
                    valid = result.returncode == 0 and not result.stderr.strip()
                except (ValueError, TypeError, KeyError):
                    record, valid = {"diagnostic_status": "ParserRejected"}, False
                # Raw streams live only in this local result and are discarded.
                result = None
                self.assertTrue(valid, "native replace comparison diagnostic rejected")
                retained = source.exists()
                same_identity = retained and identity == (source.stat().st_dev, source.stat().st_ino)
                finite = {**record, "source_identity_preserved": bool(same_identity)}
                (self.evidence / ("replace-" + candidate + ".json")).write_text(
                    json.dumps(finite, sort_keys=True) + "\n")
                self.assertTrue(record["backup_absent"] and not backup.exists(), "unexpected backup created")
                self.assertEqual(record["source_retained"], retained, "source presence observation mismatch")
                if record["outcome"] == "Passed":
                    self.assertTrue(record["target_replaced"] and not record["target_original"],
                                    "successful replacement byte observation mismatch")
                    self.assertTrue(target.read_bytes() == bytes([11, 22, 33]), "successful replacement bytes changed")
                    self.assertFalse(retained or record["source_original"], "successful replacement retained source")
                else:
                    self.assertTrue(record["target_original"] and not record["target_replaced"],
                                    "failed replacement byte observation mismatch")
                    self.assertTrue(target.read_bytes() == bytes([44, 55, 66]), "failed replacement changed target")
                    self.assertTrue(retained and same_identity and record["source_original"],
                                    "failed replacement changed source identity")
                    self.assertTrue(source.read_bytes() == bytes([11, 22, 33]), "failed replacement changed source")
                observations[candidate] = finite
            self.assertEqual(observations["TrueNull"]["outcome"], "Passed", "genuine-null owned replacement failed")
            # Ordinary-null outcome is recorded, never guessed from another OS.
            self.assertIn(observations["OrdinaryNull"]["outcome"], {"Passed", "Failed"})

    def test_cleanup_flags_survive_primary_failure_and_removal_failure_without_accounts(self):
        for removal_fails in [False, True]:
            with self.subTest(removal_fails=removal_fails):
                script = """$ErrorActionPreference='Stop'
function Remove-LocalUser {param($Name) REMOVAL}
$password=[System.Security.SecureString]::new();$created=$true;$name='nonsecret-fixture'
$removeAttempted=$false;$removeSucceeded=$false;$disposeAttempted=$false;$disposeSucceeded=$false
$caught=$false
try {try {throw 'expected primary failure'} finally {CLEANUP}} catch {$caught=$true}
ConvertTo-Json -Compress -InputObject @{caught=$caught;removal_attempted=$removeAttempted;
 removal_succeeded=$removeSucceeded;disposal_attempted=$disposeAttempted;disposal_succeeded=$disposeSucceeded}
""".replace('REMOVAL', "throw 'expected removal failure'" if removal_fails else '$null=$Name').replace(
                    'CLEANUP', probe.diagnostic_password_cleanup())
                result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
                try:
                    record = json.loads(result.stdout)
                    valid = (type(record) is dict and set(record) == {'caught','removal_attempted','removal_succeeded','disposal_attempted','disposal_succeeded'}
                             and all(type(value) is bool for value in record.values()))
                except (ValueError, TypeError):
                    valid = False
                if not valid:
                    record = {"diagnostic_status": "ParserRejected"}
                (self.evidence / ('cleanup-' + str(removal_fails) + '.json')).write_text(json.dumps(record, sort_keys=True) + '\n')
                self.assertTrue(valid and result.returncode == 0 and not result.stderr.strip(), "native cleanup flags rejected")
                self.assertTrue(record['caught'] and record['removal_attempted'] and record['disposal_attempted'] and record['disposal_succeeded'],
                                "native cleanup observation failed")
                self.assertTrue(record['removal_succeeded'] is not removal_fails, "native removal observation mismatch")


class ProtectedProbeLaunch(unittest.TestCase):
    def test_controlled_launch_metadata_rejects_payloads_and_unproven_success(self):
        record = {"status": "Passed", "phase": "Complete", "oversized_outcome": "ParameterIncorrect",
                  "oversized_length": 4000, "short_length": 140, "short_within_bound": True,
                  "child_exit": 0, "cleanup_complete": True, "cleanup_created": True,
                  "removal_attempted": True, "removal_succeeded": True,
                  "disposal_attempted": True, "disposal_succeeded": True}
        self.assertEqual(probe.parse_launch_diagnostic(json.dumps(record)), record)
        for field, value in [("password", "PRIVATE_SENTINEL"), ("path", "PRIVATE_SENTINEL"),
                             ("phase", "PRIVATE_SENTINEL"), ("oversized_length", True),
                             ("short_length", 1025), ("short_within_bound", False),
                             ("child_exit", 13), ("cleanup_complete", False),
                             ("oversized_outcome", "UnexpectedSuccess")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                probe.parse_launch_diagnostic(json.dumps({**record, field: value}))
        for text in ["PRIVATE_SENTINEL", "[]", " " * 8193]:
            with self.assertRaises(ValueError):
                probe.parse_launch_diagnostic(text)

    def test_script_identity_rejects_existing_links_and_changed_identity(self):
        with tempfile.TemporaryDirectory() as name:
            outer = Path(name)
            file = outer / 'denial-probe.ps1'
            file.write_text('fixed nonsecret fixture')
            identity = probe.validate_probe_file_identity(file)
            with self.assertRaisesRegex(RuntimeError, '^exclusive disposable probe script creation failed$'):
                probe.create_windows_probe_file(outer, 'must not overwrite', outer)
            with self.assertRaises(RuntimeError):
                probe.validate_probe_file_identity(file, (identity[0], identity[1] + 1))
            link = outer / 'linked'
            os.link(file, link)
            with self.assertRaises(RuntimeError):
                probe.validate_probe_file_identity(file)

    def test_file_acl_rejects_public_mutation_and_directory_or_reparse(self):
        segment = observed()["segments"][2]
        segment.update(directory=False, owner_class="Current", owner_trusted=True, dacl_protected=True)
        segment["aces"] = [
            {"allowed": True, "inherit_only": False, "effective_allowed": True,
             "identity_class": name, "trusted": name != "Everyone",
             "mutation_mask": 0 if name == "Everyone" else 0xD0152}
            for name in ["Current", "System", "Admins", "Everyone"]]
        probe.validate_probe_file_acl(segment)
        for field, value in [("directory", True), ("reparse", True), ("dacl_protected", False)]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                probe.validate_probe_file_acl({**segment, field: value})
        segment["aces"][-1]["mutation_mask"] = 2
        with self.assertRaises(RuntimeError):
            probe.validate_probe_file_acl(segment)

    def test_create_new_script_preserves_utf8_bom_and_literal_data_without_logging_body(self):
        with tempfile.TemporaryDirectory() as name:
            outer = Path(name)
            child = "fixed nonsecret script ' [literal] \u03bb"
            segment = observed()["segments"][2]
            segment.update(directory=False, owner_class="Current", owner_trusted=True, dacl_protected=True)
            segment["aces"] = [
                {"allowed": True, "inherit_only": False, "effective_allowed": True,
                 "identity_class": name, "trusted": name != "Everyone",
                 "mutation_mask": 0 if name == "Everyone" else 0xD0152}
                for name in ["Current", "System", "Admins", "Everyone"]]
            record = {"diagnostic_status": "Observed", "segments": [*observed()["segments"][:2], segment]}
            with patch.object(probe.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "PROBE_SCRIPT_PROTECTED", "")) as run, \
                    patch.object(probe, "capture_windows_setup_diagnostics", return_value=record):
                path, identity = probe.create_windows_probe_file(outer, child, outer)
            self.assertEqual(path.read_bytes(), b'\xef\xbb\xbf' + child.encode('utf-8'))
            self.assertEqual(probe.validate_probe_file_identity(path), identity)
            args, kwargs = run.call_args
            self.assertEqual(args[0][-1], probe.PROBE_FILE_ACL_SCRIPT)
            self.assertNotIn(str(path), args[0][-1])
            self.assertEqual(json.loads(kwargs['env']['TSD_PROBE_SCRIPT_PATH']), str(path))
            self.assertNotIn(child, (outer / 'probe-script-acl.json').read_text())


@unittest.skipUnless(os.name == "nt", "requires native Windows fixture ACLs and production admission")
class NativeWindowsOuterFixture(unittest.TestCase):
    def test_probe_script_is_protected_readable_and_rejects_public_mutation(self):
        outer = Path(tempfile.mkdtemp(prefix="tsd-denial-", dir="C:\\"))
        try:
            with tempfile.TemporaryDirectory(prefix="tsd-script-evidence-") as name:
                evidence = Path(name)
                probe.configure_windows_outer(outer, evidence)
                path, identity = probe.create_windows_probe_file(outer, "# nonsecret native fixture \u03bb\n", evidence)
                probe.validate_probe_file_identity(path, identity)
                grant = subprocess.run(["icacls", str(path), "/grant:r", "*S-1-1-0:(M)"], capture_output=True)
                self.assertTrue(grant.returncode == 0, "native script mutation control grant failed")
                record = probe.capture_windows_setup_diagnostics(outer, path, "", evidence)
                self.assertTrue(record["diagnostic_status"] == "Observed", "native script ACL read failed")
                with self.assertRaises(RuntimeError):
                    probe.validate_probe_file_acl(record["segments"][2])
        finally:
            probe.shutil.rmtree(outer)

    def test_reconstruction_removes_existing_foreign_mutation_before_production_admission(self):
        outer = Path(tempfile.mkdtemp(prefix="tsd-denial-", dir="C:\\"))
        try:
            # Deliberately add a well-known foreign writer to this owned disposable
            # directory; no ancestor or production private object is modified.
            grant = subprocess.run(["icacls", str(outer), "/grant:r", "*S-1-1-0:(OI)(CI)(M)"],
                                   capture_output=True)
            self.assertTrue(grant.returncode == 0, "native regression grant failed")
            with tempfile.TemporaryDirectory(prefix="tsd-outer-evidence-") as name:
                evidence = Path(name)
                private = outer / "private"
                before = probe.capture_windows_setup_diagnostics(outer, private, "", evidence)
                self.assertTrue(before["diagnostic_status"] == "Observed", "native regression ACL read failed")
                self.assertTrue(any(ace["effective_allowed"] and not ace["trusted"] and ace["mutation_mask"]
                                    for ace in before["segments"][1]["aces"]),
                                "native regression foreign write was absent")
                def production():
                    return subprocess.run(probe.fixture_test_command(),
                                          cwd=probe.ROOT, env={**os.environ, "TSD_USER_DENIAL_FIXTURE": str(private)},
                                          capture_output=True, text=True)
                rejected = production()
                self.assertTrue(rejected.returncode != 0 and
                                probe.fixture_failure_phase(rejected.stderr) == "NamespaceForeignMutation",
                                "production failed to reject foreign mutation")
                self.assertTrue(not private.exists(), "unsafe admission created private objects")
                probe.configure_windows_outer(outer, evidence)
                accepted = production()
                self.assertTrue(accepted.returncode == 0 and "PRIVATE_PUBLIC_OBJECTS_CREATED" in accepted.stderr,
                                "production rejected reconstructed fixture")
        finally:
            probe.shutil.rmtree(outer)


@unittest.skipUnless(os.name == "nt", "requires the native Windows SecureString runtime")
class NativeWindowsSecurePassword(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        stamp = probe.datetime.datetime.now(probe.datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        cls.evidence = probe.ROOT / "target" / ("native-user-denial-securestring-" + stamp)
        cls.evidence.mkdir(parents=True)
        probe.capture_provenance(probe.ROOT, cls.evidence)

    def test_engine_environment_metadata_is_bounded_without_module_or_path_output(self):
        script = r"""$ErrorActionPreference='Stop'
try {
 $edition=if($PSVersionTable.PSEdition -in @('Desktop','Core')){$PSVersionTable.PSEdition}else{'Other'}
 $version=$PSVersionTable.PSVersion
 $present=([Environment]::GetEnvironmentVariable('PSModulePath') -ne $null)
 $default=[IO.Path]::Combine($PSHOME,'Modules').TrimEnd([char[]]'\/')
 $included=$false
 foreach($entry in ([string]$env:PSModulePath).Split([IO.Path]::PathSeparator)) {
  if([string]::Equals($entry.TrimEnd([char[]]'\/'),$default,[StringComparison]::OrdinalIgnoreCase)){$included=$true}
 }
 ConvertTo-Json -Compress -InputObject @{edition=$edition;major=$version.Major;minor=$version.Minor;
  build=$version.Build;revision=$version.Revision;module_path_present=$present;child_default_included=$included}
} catch {exit 1}
"""
        result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
        try:
            record = parse_engine_metadata(result.stdout)
        except (ValueError, TypeError, KeyError):
            (self.evidence / "engine-environment.json").write_text('{"diagnostic_status":"ParserRejected"}\n')
            self.fail("native engine metadata was rejected")
        (self.evidence / "engine-environment.json").write_text(json.dumps(record, sort_keys=True) + "\n")
        self.assertTrue(result.returncode == 0 and not result.stderr.strip(), "native engine metadata failed")

    def run_static_check(self, script):
        result = subprocess.run(["powershell", "-NoProfile", "-Command", script],
                                capture_output=True, text=True)
        self.assertTrue(result.returncode == 0, "native SecureString check failed")
        self.assertTrue(not result.stderr.strip(), "native SecureString check emitted stderr")
        self.assertTrue(result.stdout.strip() == "SECURE_PASSWORD_CHECK_PASSED",
                        "native SecureString check emitted unexpected output")

    def test_owned_constructor_is_read_only_36_characters_and_accepted_by_pscredential(self):
        script = """$ErrorActionPreference='Stop'
$password=[System.Security.SecureString]::new()
$created=$false
try {
 PASSWORD_CONSTRUCTION
 if($password -isnot [System.Security.SecureString]){throw 'unexpected type'}
 if($password.Length -ne 36 -or -not $password.IsReadOnly()){throw 'unexpected password shape'}
 $credential=[PSCredential]::new('constructor-regression',$password)
 if($credential.Password -isnot [System.Security.SecureString]){throw 'unexpected credential type'}
} finally { PASSWORD_CLEANUP }
Write-Output 'SECURE_PASSWORD_CHECK_PASSED'
""".replace("PASSWORD_CONSTRUCTION", probe.WINDOWS_PASSWORD_CONSTRUCTION).replace(
            "PASSWORD_CLEANUP", probe.WINDOWS_PASSWORD_CLEANUP)
        self.run_static_check(script)

    def test_nondisposed_method_return_is_rejected_by_disposal_oracle(self):
        script = """$ErrorActionPreference='Stop'
$password=[System.Security.SecureString]::new()
try {
 $null=$password.IsReadOnly()
 Write-Output 'METHOD_RETURNED'
} finally {$password.Dispose()}
"""
        result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
        self.assertTrue(result.returncode == 0 and result.stdout.strip() == 'METHOD_RETURNED' and not result.stderr.strip(),
                        "native nondisposed negative control failed")
        self.assertFalse(method_proves_disposal("Returned", []))

    def test_password_is_disposed_after_construction_probe_or_account_cleanup_failure(self):
        # Simulated failures exercise the real PowerShell finally and real .NET
        # disposal; no account is created and this is not ordinary-user proof.
        for phase in ["Construction", "Probe", "Cleanup"]:
            with self.subTest(phase=phase):
                script = """$ErrorActionPreference='Stop'
FINITE_EXCEPTION_SCRIPT
$stage='Start';$caught=$false;$getterStatus='NotRun';$getterNull=$false
$getterClasses=@();$methodStatus='NotRun';$methodClasses=@();$failureClasses=@()
$status='Failed'
try {
 function Remove-LocalUser { param($Name) throw 'expected cleanup failure' }
 $password=[System.Security.SecureString]::new()
 $created=$false
 $name='disposal-regression'
 $stage='Lifecycle'
 try {
  try {
   if('PHASE' -eq 'Construction'){throw 'expected construction failure'}
   PASSWORD_CONSTRUCTION
   if('PHASE' -eq 'Probe'){throw 'expected probe failure'}
   $created=$true
  } finally { PASSWORD_CLEANUP }
 } catch { $caught=$true }
 $stage='CaughtAssertion'
 if(-not $caught){throw 'expected lifecycle failure was absent'}
 $disposed=$false
 $getterFailure=$null
 $stage='Getter'
 try { $value=$password.Length; $getterStatus='Returned';$getterNull=($null -eq $value) }
 catch [System.ObjectDisposedException] {
  $disposed=$true;$getterStatus='TypedCaught';$getterClasses=FiniteExceptionChain $_.Exception
 }
 catch { $getterStatus='OtherCaught';$getterClasses=FiniteExceptionChain $_.Exception;$getterFailure=$_ }
 # The same-object method exception is the native-proven disposal oracle.
 # Keep the original getter outcome as diagnostic metadata only.
 $stage='MethodProbe'
 try { $null=$password.IsReadOnly();$methodStatus='Returned' }
 catch { $methodStatus='Threw';$methodClasses=FiniteExceptionChain $_.Exception }
 $stage='DisposedAssertion'
 $disposed=($methodStatus -eq 'Threw' -and $methodClasses.Count -ge 1 -and
  $methodClasses.Count -le 4 -and $methodClasses[-1] -eq 'ObjectDisposed')
 for($index=0;$index -lt $methodClasses.Count-1;$index++) {
  if($methodClasses[$index] -notin @('GetValueInvocation','MethodInvocation','TargetInvocation','Runtime')){$disposed=$false}
 }
 if(-not $disposed){throw 'password disposal failed'}
 $stage='Complete';$status='Passed'
} catch { $failureClasses=FiniteExceptionChain $_.Exception }
ConvertTo-Json -Depth 4 -Compress -InputObject @{status=$status;stage=$stage;phase='PHASE';caught=$caught;
 getter_status=$getterStatus;getter_null=$getterNull;getter_exception_classes=$getterClasses;
 method_status=$methodStatus;method_exception_classes=$methodClasses;exception_classes=$failureClasses}
if($status -ne 'Passed'){exit 1}
""".replace("PHASE", phase).replace("FINITE_EXCEPTION_SCRIPT", DISPOSAL_EXCEPTION_SCRIPT).replace(
                    "PASSWORD_CONSTRUCTION", probe.WINDOWS_PASSWORD_CONSTRUCTION).replace(
                    "PASSWORD_CLEANUP", probe.WINDOWS_PASSWORD_CLEANUP)
                result = subprocess.run(["powershell", "-NoProfile", "-Command", script],
                                        capture_output=True, text=True)
                try:
                    record = parse_disposal_diagnostic(result.stdout)
                except (ValueError, TypeError, KeyError):
                    (self.evidence / ("disposal-" + phase + ".json")).write_text('{"diagnostic_status":"ParserRejected"}\n')
                    self.fail("native disposal diagnostic was rejected")
                (self.evidence / ("disposal-" + phase + ".json")).write_text(json.dumps(record, sort_keys=True) + "\n")
                if result.returncode != 0:
                    print(json.dumps(record, sort_keys=True))
                self.assertTrue(result.returncode == 0, "native SecureString check failed")
                self.assertTrue(not result.stderr.strip(), "native SecureString check emitted stderr")
                self.assertTrue(record["status"] == "Passed", "native disposal assertion failed")
                self.assertTrue(method_proves_disposal(record["method_status"], record["method_exception_classes"]),
                                "native disposed method oracle failed")


if __name__ == "__main__":
    unittest.main()
