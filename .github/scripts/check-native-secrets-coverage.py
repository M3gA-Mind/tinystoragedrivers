#!/usr/bin/env python3
"""Bind native LLVM JSON/LCOV coverage to a clean source inventory and git range."""
import argparse
import hashlib
import json
import platform
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = "fd96bea95a07abe06eb6039d2f1b8669ed0d2d09"
SOURCE = "crates/tinystoragedrivers-secrets/src/"


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def source_hashes():
    return {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (ROOT / SOURCE).rglob("*.rs")}


def normalized(filename):
    p = Path(filename)
    if not p.is_absolute():
        p = ROOT / p
    return str(p.resolve().relative_to(ROOT)).replace("\\", "/")


def changed_lines(diff):
    result, filename, line = {}, None, 0
    for text in diff.splitlines():
        if text.startswith("+++ b/"):
            filename = text[6:]
        elif text.startswith("@@"):
            line = int(re.search(r"\+(\d+)", text).group(1))
        elif text.startswith("+") and not text.startswith("+++"):
            result.setdefault(filename, set()).add(line)
            line += 1
        elif not text.startswith("-") and not text.startswith("\\"):
            line += 1
    return result


def lcov_lines(text):
    result, current = {}, None
    for line in text.splitlines():
        if line.startswith("SF:"):
            try:
                current = normalized(line[3:])
            except ValueError:
                current = None
        elif line.startswith("DA:") and current is not None:
            number, count, *_ = line[3:].split(",")
            entries = result.setdefault(current, {})
            entries[int(number)] = max(entries.get(int(number), 0), int(count))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--begin", action="store_true")
    parser.add_argument("--manifest", default="target/secrets-native-provenance.json")
    parser.add_argument("--json", default="target/secrets-native-coverage.json")
    parser.add_argument("--lcov", default="target/secrets-native-coverage.lcov")
    args = parser.parse_args()
    manifest = ROOT / args.manifest
    if git("status", "--porcelain", "--untracked-files=all"):
        raise RuntimeError("coverage requires a clean checkpoint")
    actual = {"head": git("rev-parse", "HEAD"), "base": git("rev-parse", BASE),
              "platform": platform.system(), "sources": source_hashes()}
    if args.begin:
        actual["started_ns"] = time.time_ns()
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(actual, indent=2) + "\n")
        return
    provenance = json.loads(manifest.read_text())
    for key, value in actual.items():
        if provenance.get(key) != value:
            raise RuntimeError("coverage provenance differs: " + key)
    reports = [ROOT / args.json, ROOT / args.lcov]
    if any(p.stat().st_mtime_ns < provenance["started_ns"] for p in reports):
        raise RuntimeError("coverage reports predate this execution")
    # rustc dep-info independently lists the source modules actually compiled by
    # this native instrumented test binary; a report cannot choose its own denominator.
    depfiles = list((ROOT / "target/llvm-cov-target/debug/deps").glob("tinystoragedrivers_secrets-*.d"))
    fresh = [p for p in depfiles if p.stat().st_mtime_ns >= provenance["started_ns"]]
    if not fresh:
        raise RuntimeError("missing fresh native rustc source inventory")
    inventory = set()
    for depfile in fresh:
        for name in re.findall(r"crates/tinystoragedrivers-secrets/src/[^\s:]+\.rs", depfile.read_text().replace("\\", "/")):
            if not name.endswith("_tests.rs"):
                inventory.add(name.replace("\\", "/"))
    native_module = SOURCE + ("file/windows.rs" if platform.system() == "Windows" else "file/unix.rs")
    if native_module not in inventory or not inventory:
        raise RuntimeError("native platform module missing from compiled source inventory")
    coverage = json.loads(reports[0].read_text())
    summaries = {}
    for data in coverage["data"]:
        for entry in data["files"]:
            try:
                name = normalized(entry["filename"])
            except ValueError:
                continue
            if name in inventory:
                summaries[name] = entry["summary"]["lines"]
    executable = lcov_lines(reports[1].read_text())
    changes = changed_lines(git("diff", "--unified=0", BASE, actual["head"], "--", SOURCE))
    failed, changed_count, changed_covered = [], 0, 0
    records = []
    for name in sorted(inventory):
        summary = summaries.get(name)
        if summary is None:
            # A declaration/re-export-only crate root has no LLVM counters.
            # Any function or implementation in a missing module fails closed.
            text = (ROOT / name).read_text()
            if re.search(r"\b(?:fn|impl)\b", re.sub(r"(?m)^\s*//.*$", "", text)):
                failed.append(name + ": compiled production module omitted")
            records.append({"file": name, "executable_lines": 0})
            continue
        count, covered = summary["count"], summary["covered"]
        lines = executable.get(name)
        if not isinstance(count, int) or count <= 0 or not lines:
            failed.append(name + ": empty or malformed executable coverage")
            continue
        # LLVM JSON counts generic/closure instantiations separately; LCOV
        # merges line coordinates. Both are real reports from the same profiles.
        if len(lines) > count or covered > count or any(c < 0 for c in lines.values()):
            failed.append(name + ": malformed JSON/LCOV counters")
        if covered * 100 < count * 90:
            failed.append(f"{name}: {covered}/{count} below 90%")
        selected = changes.get(name, set()) & lines.keys()
        changed_count += len(selected)
        changed_covered += sum(lines[line] > 0 for line in selected)
        records.append({"file": name, "lines": count, "covered": covered})
    if changed_count == 0 or changed_covered * 100 < changed_count * 80:
        failed.append(f"changed executable lines: {changed_covered}/{changed_count} below 80%")
    receipt = {**provenance, "compiled_production": records,
               "changed_lines": changed_count, "changed_covered": changed_covered,
               "failures": failed}
    (ROOT / "target/secrets-native-coverage-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if failed:
        raise RuntimeError("\n".join(failed))
    print(f"{actual['platform']}: {len(inventory)} compiled production files; changed {changed_covered}/{changed_count}; base={BASE} head={actual['head']}")


if __name__ == "__main__":
    main()
