"""Record the exact current source and executable before a native fixture runs."""
import hashlib
import json
import subprocess
from pathlib import Path


def capture_provenance(root, evidence, binary=None):
    paths = list((root / "crates/tinystoragedrivers-secrets/src").rglob("*.rs"))
    paths += list((root / ".github/scripts").glob("*.py"))
    if binary is not None:
        paths.append(Path(binary).resolve())
    record = {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "status": subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True),
        "sha256": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in paths},
    }
    (evidence / "execution-provenance.json").write_text(json.dumps(record, indent=2) + "\n")
