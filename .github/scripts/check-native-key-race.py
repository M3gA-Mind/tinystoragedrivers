#!/usr/bin/env python3
"""Hold a real key publisher at unlink and observe a concurrent reader."""
import argparse
import datetime
import os
import signal
import shlex
import subprocess
import tempfile
import time
from pathlib import Path

import sys
sys.dont_write_bytecode = True
from native_provenance import capture_provenance

ROOT = Path(__file__).resolve().parents[2]
TEST = "file::tests::a_key_reader_waits_for_an_in_progress_no_clobber_publication"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    evidence = (args.evidence_dir or ROOT / "target") / ("native-key-race-" + stamp)
    evidence.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="tsd-key-race-") as directory:
        key = Path(directory) / ".secret_key"
        env = {**os.environ, "TSD_KEY_RACE_FIXTURE": str(key)}
        binary = args.binary
        if binary is None:
            binary = max((p for p in (ROOT / "target/llvm-cov-target/debug/deps").glob("tinystoragedrivers_secrets-*")
                      if p.is_file() and os.access(p, os.X_OK) and "." not in p.name), key=lambda p: p.stat().st_mtime_ns)
        if args.binary is None:
            profile_env = subprocess.check_output(["cargo", "llvm-cov", "show-env"], cwd=ROOT, text=True)
            profile = next(shlex.split(line.split("=", 1)[1])[0] for line in profile_env.splitlines()
                           if line.startswith("LLVM_PROFILE_FILE="))
            directories = {Path(name).parent for name in
                           (ROOT / "target/llvm-cov-target/tinystoragedrivers-profraw-list").read_text().splitlines() if name}
            if len(directories) != 1:
                raise RuntimeError("native profile directory is ambiguous")
            env["LLVM_PROFILE_FILE"] = str(directories.pop() / Path(profile).name)
        capture_provenance(ROOT, evidence, binary)
        command = [str(binary.resolve()), TEST, "--exact", "--nocapture"]
        writer_trace = evidence / "writer.trace"
        reader_trace = evidence / "reader.trace"
        with (evidence / "writer.stdout").open("x") as out, (evidence / "writer.stderr").open("x") as err:
            writer = subprocess.Popen(["strace", "-f", "-qq", "-e", "trace=unlinkat,flock",
                "-e", "inject=unlinkat:delay_enter=5s:when=1", "-o", str(writer_trace), *command], env=env, stdout=out, stderr=err)
            stopped = None
            reader = None
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    text = writer_trace.read_text() if writer_trace.exists() else ""
                    if key.exists() and key.stat().st_nlink == 2:
                        # Freeze the tracer while its tracee is held at syscall
                        # entry. The actual two-link state stays until release.
                        os.kill(writer.pid, signal.SIGSTOP)
                        stopped = writer.pid
                        status = Path(f"/proc/{writer.pid}/status")
                        while "T (stopped)" not in status.read_text():
                            if time.monotonic() >= deadline:
                                raise RuntimeError("fixture tracer did not stop")
                            time.sleep(0.01)
                        if key.stat().st_nlink != 2:
                            raise RuntimeError("fixture did not retain the two-link state")
                        break
                    if writer.poll() is not None:
                        raise RuntimeError("writer exited before the actual two-link window")
                    time.sleep(0.01)
                if stopped is None:
                    raise RuntimeError("no actual stopped two-link key publication")
                with (evidence / "reader.stdout").open("x") as rout, (evidence / "reader.stderr").open("x") as rerr:
                    reader = subprocess.Popen(["strace", "-f", "-qq", "-e", "trace=flock", "-o", str(reader_trace), *command],
                                              env=env, stdout=rout, stderr=rerr)
                    while time.monotonic() < deadline:
                        if reader.poll() is not None:
                            break
                        text = reader_trace.read_text() if reader_trace.exists() else ""
                        if "flock(" in text:
                            break
                        time.sleep(0.01)
                    else:
                        raise RuntimeError("reader neither returned nor attempted the shared lock")
                    os.kill(stopped, signal.SIGCONT)
                    writer_code = writer.wait(timeout=30)
                    reader_code = reader.wait(timeout=30)
                (evidence / "writer.status").write_text(str(writer_code) + "\n")
                (evidence / "reader.status").write_text(str(reader_code) + "\n")
                if writer_code or reader_code:
                    raise RuntimeError("concurrent public key initialization failed; raw evidence: " + str(evidence))
                if key.stat().st_nlink != 1 or not key.read_bytes():
                    raise RuntimeError("key publication was not reconciled")
                print("Actual stopped two-link key publisher and concurrent reader succeeded: " + str(evidence))
            finally:
                if stopped is not None:
                    try:
                        os.kill(stopped, signal.SIGCONT)
                    except ProcessLookupError:
                        pass
                for process in [reader, writer]:
                    if process is not None and process.poll() is None:
                        process.terminate()
                        process.wait(timeout=30)


if __name__ == "__main__":
    main()
