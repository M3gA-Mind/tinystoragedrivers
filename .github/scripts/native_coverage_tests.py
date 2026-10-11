"""Failure-mode tests for the native compiled-source coverage gate."""
import importlib.util
import json
import os
import platform
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("gate", Path(__file__).with_name("check-native-secrets-coverage.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class NativeCoverageGate(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.module = gate.SOURCE + ("file/windows.rs" if platform.system() == "Windows" else "file/unix.rs")
        source = self.root / self.module
        source.parent.mkdir(parents=True)
        source.write_text("pub fn example() {}\n")
        self.patches = [patch.object(gate, "ROOT", self.root), patch.object(gate, "git", self.git)]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)
        self.started = time.time_ns() - 1_000_000_000
        target = self.root / "target"
        target.mkdir()
        provenance = {"head": "a" * 40, "base": gate.BASE, "platform": platform.system(),
                      "sources": gate.source_hashes(), "started_ns": self.started}
        (target / "secrets-native-provenance.json").write_text(json.dumps(provenance))
        deps = target / "llvm-cov-target/debug/deps"
        deps.mkdir(parents=True)
        (deps / "tinystoragedrivers_secrets-fixture.d").write_text("fixture: " + self.module + "\n")
        self.report = target / "secrets-native-coverage.json"
        self.lcov = target / "secrets-native-coverage.lcov"
        self.entry = {"filename": str(source), "summary": {"lines": {"count": 1, "covered": 1}}}
        self.report.write_text(json.dumps({"data": [{"files": [self.entry]}]}))
        self.lcov.write_text("SF:" + str(source) + "\nDA:1,1\nend_of_record\n")

    def git(self, *args):
        if args[0] == "status":
            return ""
        if args[:2] == ("rev-parse", "HEAD"):
            return "a" * 40
        if args[0] == "rev-parse":
            return gate.BASE
        if args[0] == "diff":
            return "+++ b/" + self.module + "\n@@ -0,0 +1 @@\n+pub fn example() {}\n"
        raise AssertionError(args)

    def run_gate(self):
        with patch.object(sys, "argv", ["gate"]):
            gate.main()

    def test_a_fresh_native_inventory_and_matching_execution_pass(self):
        self.run_gate()
        receipt = json.loads((self.root / "target/secrets-native-coverage-receipt.json").read_text())
        self.assertEqual(receipt["changed_covered"], 1)
        self.assertEqual(len(receipt["compiled_production"]), 1)

    def test_a_compiled_production_module_cannot_be_omitted(self):
        self.report.write_text(json.dumps({"data": [{"files": []}]}))
        with self.assertRaisesRegex(RuntimeError, "omitted"):
            self.run_gate()

    def test_zero_executable_coverage_fails(self):
        self.entry["summary"]["lines"] = {"count": 0, "covered": 0}
        self.report.write_text(json.dumps({"data": [{"files": [self.entry]}]}))
        with self.assertRaisesRegex(RuntimeError, "empty or malformed"):
            self.run_gate()

    def test_old_reports_do_not_prove_a_new_execution(self):
        os.utime(self.report, ns=(self.started - 1, self.started - 1))
        with self.assertRaisesRegex(RuntimeError, "predate"):
            self.run_gate()

    def test_uncovered_changed_executable_lines_fail(self):
        self.lcov.write_text("SF:" + self.entry["filename"] + "\nDA:1,0\nend_of_record\n")
        with self.assertRaisesRegex(RuntimeError, "changed executable lines: 0/1"):
            self.run_gate()

    def test_malformed_json_and_missing_reports_fail(self):
        self.report.write_text("not JSON")
        with self.assertRaises(json.JSONDecodeError):
            self.run_gate()
        self.report.unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_gate()

    def test_source_changes_invalidate_provenance(self):
        (self.root / self.module).write_text("pub fn changed() {}\n")
        with self.assertRaisesRegex(RuntimeError, "sources"):
            self.run_gate()

    def test_another_platform_inventory_is_not_native_proof(self):
        dep = self.root / "target/llvm-cov-target/debug/deps/tinystoragedrivers_secrets-fixture.d"
        dep.write_text("fixture: " + gate.SOURCE + "lib.rs\n")
        with self.assertRaisesRegex(RuntimeError, "native platform module missing"):
            self.run_gate()


if __name__ == "__main__":
    unittest.main()
