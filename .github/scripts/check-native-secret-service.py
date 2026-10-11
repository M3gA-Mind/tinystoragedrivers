#!/usr/bin/env python3
"""Exercise actual Secret Service on an owned session bus and disposable daemon.

All subprocess output is captured and discarded; receipts contain fixed booleans
only. HOME is unchanged. No inherited bus, GUI, service activation, collection or
credential is used. The daemon is killed and waited for before scratch cleanup.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import shlex
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True

from native_provenance import profile_directory

ROOT = Path(__file__).resolve().parents[2]
RECEIPT_FIELDS = {"private_bus_owner", "default_collection_unlocked",
                  "set_get_exact_readback_delete_no_entry", "locked_fixture_seeded",
                  "collection_locked", "locked_provider_denied", "owned_daemon_stopped",
                  "daemon_absent", "absent_provider_denied"}


def valid_receipt(receipt):
    return isinstance(receipt, dict) and set(receipt) == RECEIPT_FIELDS and all(value is True for value in receipt.values())


def terminate_owned_group(process):
    # The session leader can exit before its daemon descendants. Kill the group
    # after the grace period even when wait() says that the leader has exited.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        if sig == signal.SIGTERM:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
    process.wait(timeout=5)


def private_environment(state):
    env = os.environ.copy()
    for name in ("DBUS_SESSION_BUS_ADDRESS", "DBUS_STARTER_ADDRESS", "DBUS_STARTER_BUS_TYPE",
                 "GNOME_KEYRING_CONTROL", "DISPLAY", "WAYLAND_DISPLAY"):
        env.pop(name, None)
    for name, directory in (("XDG_DATA_HOME", "data"), ("XDG_RUNTIME_DIR", "runtime"),
                            ("XDG_CONFIG_HOME", "config"), ("XDG_CACHE_HOME", "cache")):
        path = state / directory
        path.mkdir(mode=0o700)
        env[name] = str(path)
    return env


def bus_configuration():
    # Deliberately no service directories: an absent daemon cannot be activated.
    return """<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-Bus Bus Configuration 1.0//EN"
"http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig><type>session</type><listen>unix:tmpdir=/tmp</listen><auth>EXTERNAL</auth>
<policy context="default"><allow send_destination="*"/><allow receive_sender="*"/>
<allow own="*"/></policy></busconfig>
"""


def captured(command, timeout=15, **kwargs):
    return subprocess.run(command, capture_output=True, timeout=timeout, check=False, **kwargs)


def dbus(method, *args, destination="org.freedesktop.secrets", path="/org/freedesktop/secrets"):
    return captured(["gdbus", "call", "--session", "--dest", destination,
                     "--object-path", path, "--method", method, *args])


def owner_is(daemon_pid):
    result = dbus("org.freedesktop.DBus.GetConnectionUnixProcessID", "org.freedesktop.secrets",
                  destination="org.freedesktop.DBus", path="/org/freedesktop/DBus")
    return result.returncode == 0 and result.stdout.decode().strip() == f"(uint32 {daemon_pid},)"


def collection_is_locked(collection, locked):
    result = dbus("org.freedesktop.DBus.Properties.Get", "org.freedesktop.Secret.Collection", "Locked",
                  path=collection)
    expected = "(<true>,)" if locked else "(<false>,)"
    return result.returncode == 0 and result.stdout.decode().strip() == expected


def provider_probe(executable, mode):
    env = {**os.environ, "TSD_ISOLATED_SECRET_SERVICE_MODE": mode}
    name = "keyring::tests::live_isolated_secret_service_probe"
    result = captured([str(executable), name, "--exact", "--nocapture"], timeout=30, env=env)
    return (result.returncode == 0 and b"1 passed; 0 failed" in result.stdout
            and name.encode() in result.stdout and not result.stderr)


def inside(state, executable):
    receipt = {}
    control = state / "control"
    control.mkdir(mode=0o700)
    daemon = None
    try:
        # A foreground owned process: no pid discovery or operator daemon reuse.
        daemon = subprocess.Popen(["gnome-keyring-daemon", "--foreground", "--unlock",
                                   "--components=secrets", "--control-directory", "control"],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=state)
        daemon.stdin.write(b"public-disposable-fixture-password\n")
        daemon.stdin.close()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and daemon.poll() is None:
            if owner_is(daemon.pid):
                break
            time.sleep(0.1)
        receipt["private_bus_owner"] = owner_is(daemon.pid)
        if not receipt["private_bus_owner"]:
            return receipt
        alias = dbus("org.freedesktop.Secret.Service.ReadAlias", "default")
        # This fresh daemon's default is its scratch login collection.
        collection = "/org/freedesktop/secrets/collection/login"
        receipt["default_collection_unlocked"] = (
            alias.returncode == 0 and alias.stdout.decode().strip() == f"(objectpath '{collection}',)"
            and collection_is_locked(collection, False))
        if not receipt["default_collection_unlocked"]:
            return receipt
        receipt["set_get_exact_readback_delete_no_entry"] = provider_probe(executable, "roundtrip")
        receipt["locked_fixture_seeded"] = provider_probe(executable, "seed")
        locked = dbus("org.freedesktop.Secret.Service.Lock", f"['{collection}']")
        receipt["collection_locked"] = locked.returncode == 0 and collection_is_locked(collection, True)
        receipt["locked_provider_denied"] = receipt["collection_locked"] and provider_probe(executable, "denied")
    finally:
        if daemon is not None:
            if daemon.poll() is None:
                daemon.terminate()
            try:
                daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=5)
            receipt["owned_daemon_stopped"] = daemon.poll() is not None
    absent = dbus("org.freedesktop.DBus.NameHasOwner", "org.freedesktop.secrets",
                  destination="org.freedesktop.DBus", path="/org/freedesktop/DBus")
    receipt["daemon_absent"] = absent.returncode == 0 and absent.stdout == b"(false,)\n"
    receipt["absent_provider_denied"] = receipt["daemon_absent"] and provider_probe(executable, "denied")
    return receipt


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--inside":
        # On an outer timeout, unwind so our daemon is also waited for normally.
        signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(1))
        try:
            receipt = inside(Path(sys.argv[2]), Path(sys.argv[3]))
            print(json.dumps(receipt, sort_keys=True))
            return 0 if valid_receipt(receipt) else 1
        except (OSError, subprocess.SubprocessError, UnicodeError):
            print('{"native_probe_failed": true}')
            return 1
    coverage = "--coverage" in sys.argv
    binaries = ROOT / ("target/llvm-cov-target/debug/deps" if coverage else "target/debug/deps")
    selected = os.environ.get("TSD_SECRET_SERVICE_PROBE")
    if selected:
        executable = Path(selected).resolve()
    else:
        candidates = [p for p in binaries.glob("tinystoragedrivers_secrets-*")
                      if p.is_file() and os.access(p, os.X_OK) and "." not in p.name]
        if not candidates:
            print("isolated_secret_service_admitted=false")
            return 1
        executable = max(candidates, key=lambda p: p.stat().st_mtime_ns).resolve()
    fixture_root = ROOT / "target"
    fixture_root.mkdir(exist_ok=True)
    artifact = Path(tempfile.mkdtemp(prefix="native-secret-service-", dir=fixture_root))
    state = artifact / "state"
    state.mkdir(mode=0o700)
    try:
        env = private_environment(state)
        if coverage:
            result = captured(["cargo", "llvm-cov", "show-env"], cwd=ROOT)
            profile = next(shlex.split(line.split("=", 1)[1])[0]
                           for line in result.stdout.decode().splitlines()
                           if line.startswith("LLVM_PROFILE_FILE="))
            if result.returncode:
                print("isolated_secret_service_admitted=false")
                return 1
            env["LLVM_PROFILE_FILE"] = str(profile_directory(ROOT) / Path(profile).name)
        config = state / "bus.conf"
        config.write_text(bus_configuration())
        command = ["dbus-run-session", "--config-file", str(config), "--", sys.executable,
                   str(Path(__file__).resolve()), "--inside", str(state), str(executable)]
        # A private process group also owns the bus and daemon on outer timeout.
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   env=env, start_new_session=True)
        try:
            output, _ = process.communicate(timeout=100)
        except subprocess.TimeoutExpired:
            terminate_owned_group(process)
            raise
        result = subprocess.CompletedProcess(command, process.returncode, output)
        receipt = json.loads(result.stdout)
        valid = result.returncode == 0 and valid_receipt(receipt)
        # Never persist an unexpected child payload, even on failure.
        safe_receipt = {name: receipt.get(name) is True for name in RECEIPT_FIELDS} if isinstance(receipt, dict) else {"native_probe_failed": True}
        (artifact / "receipt.json").write_text(json.dumps(safe_receipt, indent=2, sort_keys=True) + "\n")
        print("isolated_secret_service_admitted=" + str(valid).lower())
        return 0 if valid else 1
    except (OSError, subprocess.SubprocessError, ValueError, StopIteration):
        (artifact / "receipt.json").write_text('{"native_probe_failed": true}\n')
        print("isolated_secret_service_admitted=false")
        return 1
    finally:
        try:
            shutil.rmtree(state)
        except OSError:
            print("isolated_secret_service_cleanup=false")
            raise SystemExit(1) from None


if __name__ == "__main__":
    sys.exit(main())
