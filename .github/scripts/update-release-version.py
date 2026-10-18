"""Bump a release, validate every independent workspace, then stage exact paths.

Run from the repository root with VERSION PACKAGE. Cargo's workspace update
preserves unrelated locked dependencies where possible. No paths are staged
until every resolver succeeds, so a failed proof prevents the release commit.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("package")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version):
        parser.error("version must be MAJOR.MINOR.PATCH")
    root = Path("Cargo.toml")
    text, replacements = re.subn(
        r'(\[workspace\.package\][\s\S]*?\nversion = ")[^"]+(")',
        lambda match: match[1] + args.version + match[2], root.read_text(), count=1,
    )
    if replacements != 1:
        parser.error("workspace.package.version was not found")
    root.write_text(text)
    manifests = [root, *sorted(Path("proof-workspaces").glob("*/Cargo.toml"))]
    for manifest in manifests:
        # Each proof owns an independent resolution graph and Cargo.lock.
        subprocess.run(["cargo", "update", "--workspace", "--manifest-path", str(manifest)], check=True)
    for manifest in manifests:
        for features in [[], ["--all-features"]]:
            result = subprocess.run(
                ["cargo", "metadata", "--format-version", "1", "--locked",
                 "--manifest-path", str(manifest), *features],
                check=True, stdout=subprocess.PIPE, text=True,
            )
            if manifest == root:
                packages = json.loads(result.stdout)["packages"]
                version = next((p["version"] for p in packages if p["name"] == args.package), None)
                if version != args.version:
                    parser.error(f"version bump did not take: expected {args.version}, got {version}")
    subprocess.run(["git", "add", "--", str(root),
                    *[str(manifest.with_name("Cargo.lock")) for manifest in manifests]], check=True)


if __name__ == "__main__":
    main()
