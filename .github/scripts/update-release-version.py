"""Bump a release, validate every independent workspace, then stage exact paths.

Run from the repository root with VERSION PACKAGE. Cargo's workspace update
preserves unrelated locked dependencies where possible. Internal path-dependency
requirements move with the workspace package version; external pins stay fixed.
No paths are staged until every resolver succeeds. Failed updates or validation
restore every changed manifest and lockfile to its original bytes and existence.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import tomllib


DEPENDENCY_KEYS = ("dependencies", "dev-dependencies", "build-dependencies")


def _dependency_specs(document):
    """Yield dependency specifications, including target-specific tables."""
    for key in DEPENDENCY_KEYS:
        yield from document.get(key, {}).values()
    yield from document.get("workspace", {}).get("dependencies", {}).values()
    for target in document.get("target", {}).values():
        yield from _dependency_specs(target)


def _workspace_package_dirs(root):
    """Map workspace package directories to package names without resolving deps."""
    document = tomllib.loads(root.read_text())
    workspace = document.get("workspace", {})
    members = workspace.get("members", [])
    excluded = workspace.get("exclude", [])
    directories = {}
    for pattern in members:
        for candidate in root.parent.glob(pattern):
            if not candidate.is_dir() or any(candidate.match(item) for item in excluded):
                continue
            manifest = candidate / "Cargo.toml"
            if manifest.is_file():
                package = tomllib.loads(manifest.read_text()).get("package")
                if package and package.get("name"):
                    directories[candidate.resolve()] = package["name"]
    return directories


def _is_internal_path_dependency(spec, manifest, package_dirs):
    if not isinstance(spec, dict) or not isinstance(spec.get("path"), str):
        return False
    target = (manifest.parent / spec["path"]).resolve()
    if target.is_file():
        target = target.parent
    return target in package_dirs


def _rewrite_path_dependency_versions(source, manifest, package_dirs, version):
    """Update only version requirements attached to workspace path dependencies."""
    lines = source.splitlines(keepends=True)
    section_start = None

    def rewrite_inline(line):
        def replace_table(match):
            table = match.group(0)
            try:
                spec = tomllib.loads("dependency = " + table)["dependency"]
            except tomllib.TOMLDecodeError:
                return table
            if not _is_internal_path_dependency(spec, manifest, package_dirs):
                return table
            return re.sub(r'(\bversion\s*=\s*["\'])[^"\']+(["\'])',
                          lambda found: found[1] + version + found[2], table, count=1)

        return re.sub(r"\{[^{}\n]*\}", replace_table, line)

    def rewrite_section(section):
        header = section[0]
        try:
            parsed = tomllib.loads(header + "\n" + "".join(section[1:]))
        except tomllib.TOMLDecodeError:
            return section
        if not any(_is_internal_path_dependency(spec, manifest, package_dirs)
                   for spec in _dependency_specs(parsed)):
            return section
        rewritten = [section[0], *[rewrite_inline(line) for line in section[1:]]]
        return [re.sub(r'^(\s*version\s*=\s*["\'])[^"\']+(["\'])',
                       lambda found: found[1] + version + found[2], line, count=1)
                for line in rewritten]

    output = []
    for line in lines:
        if re.match(r"^\s*\[\[?.+\]\]?\s*(?:#.*)?$", line):
            if section_start is not None:
                output.extend(rewrite_section(section_start))
            section_start = [line]
        elif section_start is not None:
            section_start.append(line)
        else:
            output.append(rewrite_inline(line))
    if section_start is not None:
        output.extend(rewrite_section(section_start))
    return "".join(output)


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
    manifests = [root, *sorted(Path("proof-workspaces").glob("*/Cargo.toml"))]
    package_dirs = _workspace_package_dirs(root)
    package_manifests = [
        (directory / "Cargo.toml").relative_to(root.parent.resolve())
        for directory in sorted(package_dirs)
    ]
    internal_manifests = [root, *package_manifests,
                          *[manifest for manifest in manifests if manifest != root]]
    affected = [root, *[manifest.with_name("Cargo.lock") for manifest in manifests]]
    # Capture local edits and absent locks before the first mutation. Git's
    # index is not a preimage: the operator may have unstaged changes here.
    preimages = {path: path.read_bytes() if path.exists() else None
                 for path in [*affected, *internal_manifests]}
    updated_manifests = [root]
    try:
        text = _rewrite_path_dependency_versions(text, root, package_dirs, args.version)
        root.write_text(text)
        for manifest in internal_manifests:
            if manifest == root or not manifest.exists():
                continue
            original = manifest.read_text()
            updated = _rewrite_path_dependency_versions(
                original, manifest, package_dirs, args.version
            )
            if updated != original:
                manifest.write_text(updated)
                updated_manifests.append(manifest)
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
    except BaseException as failure:
        rollback_errors = []
        for path, contents in preimages.items():
            try:
                if contents is None:
                    path.unlink(missing_ok=True)
                else:
                    path.write_bytes(contents)
            except OSError as error:
                rollback_errors.append(f"{path}: {error}")
        if rollback_errors:
            raise RuntimeError("release update failed; rollback also failed: "
                               + "; ".join(rollback_errors)) from failure
        raise
    subprocess.run(["git", "add", "--", *[str(path) for path in updated_manifests],
                    *[str(manifest.with_name("Cargo.lock")) for manifest in manifests]], check=True)


if __name__ == "__main__":
    main()
