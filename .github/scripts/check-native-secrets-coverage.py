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


def production(name):
    return name.startswith(SOURCE) and not name.endswith("_tests.rs")


def lcov_lines(text):
    result, current, seen = {}, None, set()
    for line in text.splitlines():
        if line.startswith("SF:"):
            if current is not None:
                raise RuntimeError("unterminated LCOV file record")
            try:
                current = normalized(line[3:])
            except ValueError:
                current = None
            if current is not None:
                if current in seen:
                    raise RuntimeError("duplicate LCOV file record")
                seen.add(current)
                result[current] = {}
        elif line == "end_of_record":
            current = None
        elif line.startswith("DA:") and current is not None:
            number, count, *_ = line[3:].split(",")
            number, count = int(number), int(count)
            entries = result[current]
            if number <= 0 or count < 0 or number in entries:
                raise RuntimeError("duplicate or malformed LCOV coordinate")
            entries[number] = count
    if current is not None:
        raise RuntimeError("unterminated LCOV file record")
    return result


def json_lines(entry):
    """Map LLVM's merged non-gap segments to unique executable line counts.

    Each segment describes coverage up to the next segment. An endpoint at
    column one belongs to the following interval, not this one. LLVM summary
    lines count overlapping function/generic instantiations separately; they
    must not be substituted for the independently mapped coordinate set.
    """
    segments = entry.get("segments")
    if not isinstance(segments, list) or not segments:
        raise RuntimeError("missing executable JSON segments")
    previous = (0, 0)
    for segment in segments:
        if not isinstance(segment, list) or len(segment) != 6:
            raise RuntimeError("malformed JSON coverage segment")
        line, column, count, has_count, region_entry, gap = segment
        if (any(type(value) is not int for value in [line, column, count])
                or min(line, column) <= 0 or count < 0
                or any(type(value) is not bool for value in [has_count, region_entry, gap])
                or (line, column) <= previous):
            raise RuntimeError("malformed JSON coverage segment")
        previous = (line, column)
    if segments[-1][3]:
        raise RuntimeError("unterminated JSON coverage segment")
    result = {}
    for start, end in zip(segments, segments[1:]):
        if start[3] and not start[5]:
            stop = end[0] - int(end[1] == 1)
            for line in range(start[0], stop + 1):
                result[line] = max(result.get(line, 0), start[2])
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
    summaries, coordinates = {}, {}
    for data in coverage["data"]:
        for entry in data["files"]:
            try:
                name = normalized(entry["filename"])
            except ValueError:
                continue
            if production(name):
                if name not in inventory:
                    raise RuntimeError("JSON production source differs from compiled inventory")
                if name in summaries:
                    raise RuntimeError("duplicate JSON source entry")
                summaries[name] = entry["summary"]["lines"]
                coordinates[name] = json_lines(entry)
    executable = lcov_lines(reports[1].read_text())
    if {name for name in executable if production(name)} != set(coordinates):
        raise RuntimeError("compiled production module omitted or JSON/LCOV production source sets differ")
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
        if type(count) is not int or type(covered) is not int or count <= 0 or covered < 0 or not lines:
            failed.append(name + ": empty or malformed executable coverage")
            continue
        # Unique merged coordinates and counters must agree exactly across
        # both reports, independent of the instantiated JSON summary count.
        if lines != coordinates[name]:
            failed.append(name + ": JSON/LCOV executable coordinates or counters differ")
        unique_covered = sum(value > 0 for value in coordinates[name].values())
        unique_uncovered = len(coordinates[name]) - unique_covered
        # Each merged covered/uncovered coordinate must be contributed by at
        # least one function/instantiation counted in the summary. Additional
        # overlapping instantiations are valid; impossible lower totals are not.
        if (covered > count or covered < unique_covered
                or count - covered < unique_uncovered):
            failed.append(name + ": malformed JSON/LCOV counters")
        if covered * 100 < count * 90:
            failed.append(f"{name}: {covered}/{count} below 90%")
        selected = changes.get(name, set()) & coordinates[name].keys()
        changed_count += len(selected)
        changed_covered += sum(coordinates[name][line] > 0 for line in selected)
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
