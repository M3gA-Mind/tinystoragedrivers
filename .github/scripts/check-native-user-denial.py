#!/usr/bin/env python3
"""Attempt actual read/write/replace/delete as a separate standard native user.

The Windows lane creates and removes only its randomly named fixture account.
Unix uses the existing unprivileged nobody account. A readable public control
proves traversal and process execution before private-object denial is accepted.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import sys
sys.dont_write_bytecode = True
from native_provenance import capture_provenance

ROOT = Path(__file__).resolve().parents[2]
TEST = "file::tests::published_objects_are_ready_for_a_separate_user_access_probe"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    evidence = (args.evidence_dir or ROOT / "target") / ("native-user-denial-" + stamp)
    evidence.mkdir(parents=True)
    capture_provenance(ROOT, evidence)
    windows = os.name == "nt"
    # Windows fixture ancestors must be independent of an administrator profile.
    outer = Path(tempfile.mkdtemp(prefix="tsd-denial-", dir="C:\\" if windows else str(Path("/var/tmp").resolve())))
    try:
        if windows:
            subprocess.run(["icacls", str(outer), "/inheritance:r", "/grant:r",
                            f"{os.environ['USERNAME']}:(OI)(CI)(F)", "*S-1-1-0:(OI)(CI)(RX)"],
                           capture_output=True, check=True)
        else:
            outer.chmod(0o755)
        public = outer / "public"
        public.mkdir()
        if windows:
            subprocess.run(["icacls", str(public), "/grant:r", "*S-1-1-0:(OI)(CI)(M)"], capture_output=True, check=True)
        else:
            public.chmod(0o777)
        control = public / "control"
        control.write_text("public-fixture-control")
        if not windows:
            control.chmod(0o666)
        private = outer / "private"
        env = {**os.environ, "TSD_USER_DENIAL_FIXTURE": str(private)}
        result = subprocess.run(["cargo", "test", "--locked", "-p", "tinystoragedrivers-secrets",
                                 "--lib", TEST, "--", "--exact", "--nocapture"],
                                cwd=ROOT, env=env, capture_output=True, text=True)
        (evidence / "fixture.stdout").write_text(result.stdout)
        (evidence / "fixture.stderr").write_text(result.stderr)
        (evidence / "fixture.status").write_text(str(result.returncode) + "\n")
        if result.returncode or "PRIVATE_PUBLIC_OBJECTS_CREATED" not in result.stderr:
            raise RuntimeError("production fixture setup failed")
        files = [str(private / name) for name in
                 [".secret_key", "secrets.enc", "secrets.enc.lock", ".secret_key.lock"]]
        originals = {name: Path(name).read_bytes() for name in files}
        if windows:
            def literal(value):
                return "'" + value.replace("'", "''") + "'"
            child = """$ErrorActionPreference='Stop'
$p=[Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
if($p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 11}
$control=CONTROL
if([IO.File]::ReadAllText($control) -ne 'public-fixture-control'){exit 12}
[IO.File]::WriteAllText($control, 'public-fixture-control')
$source=$control+'.replacement'
[IO.File]::WriteAllText($source, 'public-fixture-control')
[IO.File]::Replace($source,$control,$null)
[IO.File]::Delete($control)
if([IO.File]::Exists($control)){exit 14}
foreach($f in @(FILES)) {
 try { $h=[IO.File]::OpenRead($f); $h.Dispose(); exit 13 }
 catch [UnauthorizedAccessException] { }
 try { $h=[IO.File]::Open($f,[IO.FileMode]::Open,[IO.FileAccess]::Write); $h.Dispose(); exit 15 }
 catch [UnauthorizedAccessException] { }
 [IO.File]::WriteAllText($source, 'public-fixture-control')
 try { [IO.File]::Replace($source,$f,$null); exit 16 }
 catch [UnauthorizedAccessException] { }
 if(-not [IO.File]::Exists($source)){exit 17}
 [IO.File]::Delete($source)
 try { [IO.File]::Delete($f); exit 18 }
 catch [UnauthorizedAccessException] { }
}
Write-Output 'ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED'
""".replace("CONTROL", literal(str(control))).replace("FILES", ",".join(map(literal, files)))
            encoded = base64.b64encode(child.encode("utf-16le")).decode()
            script = """$ErrorActionPreference='Stop'
$name='tsd'+[Guid]::NewGuid().ToString('N').Substring(0,12)
$password=ConvertTo-SecureString ([Guid]::NewGuid().ToString('N')+'aA!7') -AsPlainText -Force
$created=$false
try {
 New-LocalUser -Name $name -Password $password | Out-Null; $created=$true
 Add-LocalGroupMember -Group (Get-LocalGroup -SID 'S-1-5-32-545') -Member $name
 $credential=[PSCredential]::new(($env:COMPUTERNAME+'\\'+$name),$password)
 $p=Start-Process -FilePath "$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" -Credential $credential -ArgumentList @('-NoProfile','-EncodedCommand','ENCODED') -Wait -PassThru -RedirectStandardOutput OUT -RedirectStandardError ERR
 if($p.ExitCode -ne 0){exit $p.ExitCode}
 Get-Content OUT
} finally {if($created){Remove-LocalUser -Name $name}}
""".replace("ENCODED", encoded).replace("OUT", literal(str(evidence / "child.stdout"))).replace("ERR", literal(str(evidence / "child.stderr")))
            result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
        else:
            child = """import os,sys
assert os.geteuid() != int(sys.argv[1]) and os.geteuid() != 0
control=sys.argv[2]
assert open(control).read() == 'public-fixture-control'
with open(control,'r+b') as handle: handle.write(b'public-fixture-control')
source=control+'.replacement'
with open(source,'wb') as handle: handle.write(b'public-fixture-control')
os.replace(source,control)
assert open(control).read() == 'public-fixture-control'
os.unlink(control)
assert not os.path.exists(control)
for name in sys.argv[3:]:
 for mode in ['rb','r+b']:
  try:
   with open(name,mode): pass
  except PermissionError: continue
  raise RuntimeError('private open unexpectedly succeeded')
 with open(source,'wb') as handle: handle.write(b'public-fixture-control')
 try: os.replace(source,name)
 except PermissionError: pass
 else: raise RuntimeError('private replacement unexpectedly succeeded')
 assert os.path.exists(source)
 os.unlink(source)
 try: os.unlink(name)
 except PermissionError: pass
 else: raise RuntimeError('private deletion unexpectedly succeeded')
print('ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED')
"""
            result = subprocess.run(["sudo", "-n", "-u", "nobody", "--", "/usr/bin/python3", "-c", child,
                                     str(os.geteuid()), str(control), *files], capture_output=True, text=True)
        (evidence / "probe.stdout").write_text(result.stdout)
        (evidence / "probe.stderr").write_text(result.stderr)
        (evidence / "probe.status").write_text(str(result.returncode) + "\n")
        if result.returncode or "ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED" not in result.stdout:
            raise RuntimeError("actual separate-user denial was not proven: " + str(evidence))
        if any(not Path(name).is_file() or Path(name).read_bytes() != before for name, before in originals.items()):
            raise RuntimeError("private objects changed during the denial probe")
        (evidence / "receipt.json").write_text(json.dumps({"platform":os.name,"objects":4,
             "public_control_operations":["read", "write", "replace", "delete"],
             "private_operations_denied":16,"private_objects_unchanged":True}, indent=2)+"\n")
        print("Actual separate standard user denied all sixteen private object operations: " + str(evidence))
    finally:
        shutil.rmtree(outer)


if __name__ == "__main__":
    main()
