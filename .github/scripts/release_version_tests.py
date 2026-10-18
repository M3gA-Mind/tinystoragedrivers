"""Exercise release lockfiles with real offline Cargo resolution and Git staging."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / ".github/scripts/update-release-version.py"


class ReleaseLocks(unittest.TestCase):
    def setUp(self):
        directory = ROOT / "target/release-lock-fix"
        directory.mkdir(parents=True, exist_ok=True)
        self.fixture = tempfile.TemporaryDirectory(dir=directory)
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n'
                   'resolver = "2"\n[workspace.package]\nversion = "1.0.0"\n')
        for name in ["facade", "leaf"]:
            dependency = '\n[dependencies]\nleaf = { path = "../leaf" }\n' if name == "facade" else ""
            self.write(f"crates/{name}/Cargo.toml", f'[package]\nname = "{name}"\n'
                       f'version.workspace = true\nedition = "2021"\n{dependency}')
            self.write(f"crates/{name}/src/lib.rs", "//! Fixture.\n")
        for name in ["first", "future-proof"]:
            self.write(f"proof-workspaces/{name}/Cargo.toml", '[workspace]\n'
                       f'[package]\nname = "{name}"\nversion = "0.0.0"\nedition = "2021"\n'
                       '[dependencies]\nfacade = { path = "../../crates/facade" }\n')
            self.write(f"proof-workspaces/{name}/src/lib.rs", "//! Independent consumer.\n")
        self.manifests = [Path("Cargo.toml"), *[p.relative_to(self.root) for p in
                          sorted(self.root.glob("proof-workspaces/*/Cargo.toml"))]]
        self.run_command("git", "init", "--quiet")
        for manifest in self.manifests:
            self.metadata(manifest)
        self.run_command("git", "add", ".")
        self.run_command("git", "-c", "user.name=Release test", "-c", "user.email=test@example.invalid",
                         "commit", "--quiet", "-m", "Initial fixture")

    def write(self, path, contents):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents)

    def run_command(self, *command, check=True, env=None):
        result = subprocess.run(command, cwd=self.root,
                                env={**os.environ, "CARGO_NET_OFFLINE": "true", **(env or {})},
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and result.returncode:
            self.fail(f"{command}: exit {result.returncode}\n{result.stdout}\n{result.stderr}")
        return result

    def metadata(self, manifest, locked=False):
        command = ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)]
        if locked:
            command.append("--locked")
        return self.run_command(*command)

    def test_major_bump_updates_validates_and_stages_every_workspace_lock(self):
        self.write("unrelated.txt", "leave this unstaged\n")
        self.run_command(sys.executable, str(HELPER), "2.0.0", "facade")
        for manifest in self.manifests:
            self.metadata(manifest, locked=True)
            lock = (self.root / manifest).with_name("Cargo.lock").read_text()
            self.assertIn('version = "2.0.0"', lock)
            self.assertNotIn('version = "1.0.0"', lock)
        staged = self.run_command("git", "diff", "--cached", "--name-only").stdout.splitlines()
        self.assertEqual(sorted(staged), sorted(["Cargo.toml", *[
            str(manifest.with_name("Cargo.lock")) for manifest in self.manifests]]))
        self.assertEqual(self.run_command("git", "diff", "--name-only").stdout, "")

    def test_failed_proof_update_stops_before_staging(self):
        self.write("proof-workspaces/future-proof/Cargo.toml", '[workspace]\n[package]\n'
                   'name = "broken"\nversion = "0.0.0"\n[dependencies]\n'
                   'missing = { path = "../../missing" }\n')
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "facade", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")

    def test_wrong_release_package_stops_before_staging(self):
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "absent", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("version bump did not take", result.stderr)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")

    def test_failed_locked_proof_validation_stops_before_staging(self):
        # Fault injection only removes a proof lock after update. Every command
        # still executes real Cargo, including the locked resolver rejection.
        cargo = shutil.which("cargo")
        self.assertIsNotNone(cargo)
        self.write("bin/cargo", f"#!{sys.executable}\n"
                   "import os, sys\nfrom pathlib import Path\n"
                   "if 'metadata' in sys.argv and '--locked' in sys.argv "
                   "and 'proof-workspaces/first/Cargo.toml' in sys.argv:\n"
                   "    Path('proof-workspaces/first/Cargo.lock').unlink()\n"
                   f"os.execv({cargo!r}, [{cargo!r}, *sys.argv[1:]])\n")
        (self.root / "bin/cargo").chmod(0o755)
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "facade", check=False,
                                  env={"PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"]})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--locked", result.stderr)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")


if __name__ == "__main__":
    unittest.main()
