#!/usr/bin/env python3
"""Observe real Linux fsync failure after ledger publication and exact retry."""
import argparse
import datetime
import shlex
import json
import os
import re
import subprocess
from pathlib import Path

import sys
sys.dont_write_bytecode = True
from native_provenance import capture_provenance

ROOT = Path(__file__).resolve().parents[2]
TEST = "file::migration::tests::native_io_fault_preserves_source_and_reconciles_publication"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    evidence = ROOT / ("target/native-io-fault-" + stamp)
    evidence.mkdir(parents=True, exist_ok=False)
    binary = args.binary
    if binary is None:
        binaries = [p for p in (ROOT / "target/llvm-cov-target/debug/deps").glob("tinystoragedrivers_secrets-*")
                    if p.is_file() and os.access(p, os.X_OK) and "." not in p.name]
        binary = max(binaries, key=lambda p: p.stat().st_mtime_ns)
    capture_provenance(ROOT, evidence, binary)
    command = [str(binary.resolve()), TEST, "--exact", "--nocapture"]
    profile_env = subprocess.check_output(["cargo", "llvm-cov", "show-env"], cwd=ROOT, text=True)
    profile = next(shlex.split(line.split("=", 1)[1])[0] for line in profile_env.splitlines()
                   if line.startswith("LLVM_PROFILE_FILE="))
    report_profiles = (ROOT / "target/llvm-cov-target/tinystoragedrivers-profraw-list").read_text().splitlines()
    directories = {Path(name).parent for name in report_profiles if name}
    if len(directories) != 1:
        raise RuntimeError("native report profile directory is ambiguous")
    profile = str(directories.pop() / Path(profile).name)
    environment = {**os.environ, "LLVM_PROFILE_FILE": profile}
    trace = evidence / "baseline.trace"
    base = subprocess.run(["strace", "-f", "-qq", "-yy", "-e", "trace=fsync,renameat", "-o", str(trace), *command],
                          env=environment, capture_output=True, text=True, check=True)
    (evidence / "baseline.stdout").write_text(base.stdout)
    (evidence / "baseline.stderr").write_text(base.stderr)
    # Select the actual worker's first directory barrier after a ledger rename;
    # count fsync ordinals per tracee because strace injection uses that scope.
    counters, published, ordinal = {}, set(), None
    for line in trace.read_text().splitlines():
        match = re.match(r"(\d+)\s+(.*)", line)
        if not match:
            continue
        pid, call = match.groups()
        if call.startswith("renameat(") and '"secrets.enc.imports"' in call and call.endswith("= 0"):
            published.add(pid)
        if call.startswith("fsync("):
            counters[pid] = counters.get(pid, 0) + 1
            if pid in published:
                ordinal = counters[pid]
                break
    if ordinal is None:
        raise RuntimeError("no observed native ledger directory barrier")
    observed_total = 0
    for label, syscall, selected in [("stage-flush", "fsync", 1),
                                     ("publication-barrier", "fsync", ordinal),
                                     ("published-file-flush", "fsync", ordinal + 1),
                                     ("rename", "renameat", 1)]:
        injected = evidence / (label + ".trace")
        env = {**environment, "TSD_EXPECT_NATIVE_IO_FAULT": "1"}
        result = subprocess.run(["strace", "-f", "-qq", "-yy", "-e", "trace=fsync,renameat",
                                 "-e", f"inject={syscall}:error=EIO:when={selected}", "-o", str(injected), *command],
                                env=env, capture_output=True, text=True)
        (evidence / (label + ".stdout")).write_text(result.stdout)
        (evidence / (label + ".stderr")).write_text(result.stderr)
        (evidence / (label + ".status")).write_text(str(result.returncode) + "\n")
        observed = [line for line in injected.read_text().splitlines()
                    if syscall + "(" in line and "EIO" in line and "INJECTED" in line]
        if result.returncode or "OBSERVED_NATIVE_IO_ERROR=5" not in result.stdout or not observed:
            raise RuntimeError("actual native injected failure/reconciliation was not observed: " + label)
        observed_total += len(observed)
    key_test = "file::tests::native_key_publication_faults_preserve_an_uncertain_winner"
    key_command = [str(binary.resolve()), key_test, "--exact", "--nocapture"]
    for label, syscall, key_ordinal in [("link", "linkat", 1), ("unlink", "unlinkat", 1),
                                    ("dir-sync", "fsync", 2), ("file-sync", "fsync", 3)]:
        trace = evidence / ("key-" + label + ".trace")
        result = subprocess.run(["strace", "-f", "-qq", "-yy", "-e", "trace=fsync,linkat,unlinkat",
                                 "-e", f"inject={syscall}:error=EIO:when={key_ordinal}", "-o", str(trace), *key_command],
                                env={**environment, "TSD_EXPECT_NATIVE_KEY_FAULT": label}, capture_output=True, text=True)
        (evidence / ("key-" + label + ".stdout")).write_text(result.stdout)
        (evidence / ("key-" + label + ".stderr")).write_text(result.stderr)
        (evidence / ("key-" + label + ".status")).write_text(str(result.returncode) + "\n")
        if result.returncode or "OBSERVED_NATIVE_IO_ERROR=5" not in result.stdout or "INJECTED" not in trace.read_text():
            raise RuntimeError("native key publication failure not observed: " + label)
        observed_total += 1
    # Raw traces contain disposable fixture paths only. No read/write payloads
    # are traced; no operator namespace is inspected or credential material logged.
    receipt = {"boundary": "native fsync API error, not physical power loss",
               "syscalls": ["fsync", "renameat"], "observed_ordinal": ordinal, "injected_calls": observed_total,
               "test": TEST, "exit": result.returncode}
    (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
