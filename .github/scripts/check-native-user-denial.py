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
import re
import shutil
import subprocess
import tempfile

import sys
sys.dont_write_bytecode = True
from native_provenance import capture_provenance

ROOT = Path(__file__).resolve().parents[2]
TEST = "file::tests::published_objects_are_ready_for_a_separate_user_access_probe"
MUTATION_MASK = 0x500D0152
IDENTITY_CLASSES = {"Current", "System", "Admins", "TrustedInstaller", "Everyone", "AuthUsers", "BuiltinUsers", "Owner", "Other"}
READ_STAGES = {"Existence", "Item", "Acl", "Descriptor", "Owner", "DaclLimit",
               "AceType", "AceFlags", "AceIdentity", "AceMask", "AceClassification",
               "DaclShape", "ItemMetadata", "OwnerClassification", "Record"}
SCRIPT_STAGES = {"Start", "Identity", "Paths", "Segments", "Serialize", "Complete"}
ERROR_CATEGORIES = {"ParserError", "InvalidArgument", "ObjectNotFound", "PermissionDenied",
                    "InvalidOperation", "ResourceUnavailable", "NotSpecified", "Other"}

# Paths enter only through the child environment and Get-Acl's literal argument.
# SID values remain inside the child; even error output is never copied.
ACL_DIAGNOSTIC_SCRIPT = r"""$ErrorActionPreference='Stop'
$stage='Start'
try {
$stage='Identity'
$current=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$trustedInstaller='S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'
function Trusted($sid) {
 return ($sid -eq $current -or $sid -eq 'S-1-5-18' -or $sid -eq 'S-1-5-32-544' -or $sid -eq $trustedInstaller)
}
function Classify($sid,$owner) {
 if($sid -eq $current){return 'Current'}
 switch($sid) {
  'S-1-5-18' {return 'System'}
  'S-1-5-32-544' {return 'Admins'}
  'S-1-1-0' {return 'Everyone'}
  'S-1-5-11' {return 'AuthUsers'}
  'S-1-5-32-545' {return 'BuiltinUsers'}
 }
 if($sid -eq $trustedInstaller){return 'TrustedInstaller'}
 if($owner -and $sid -eq $owner){return 'Owner'}
 return 'Other'
}
$stage='Paths'
# Direct assignment keeps Windows PowerShell's root array, without collecting
# it into a second array. Invalid input must fail before any literal-path read.
try { $paths=ConvertFrom-Json -InputObject $env:TSD_ACL_DIAGNOSTIC_PATHS }
catch { $paths=$null }
$roles=@('Volume','Outer','Private')
$validPaths=($paths -is [array] -and $paths.Count -eq 3)
foreach($path in $paths) {
 if($path -isnot [string] -or $path.Length -eq 0){$validPaths=$false}
}
if(-not $validPaths -or $paths.Count -ne 3){
 ConvertTo-Json -Compress -InputObject @{status='ScriptFailed';stage='Paths';category='InvalidArgument'}
 exit 21
}
$stage='Segments'
$segments=@()
for($ordinal=0;$ordinal -lt 3;$ordinal++) {
 $record=@{ordinal=$ordinal;role=$roles[$ordinal];status='ReadFailed'}
 $readStage='Existence'
 $readCategory=$null
 try {
  if(-not (Test-Path -LiteralPath $paths[$ordinal])) {
   $record.status='Missing'
  } else {
   $readStage='Item'
   $item=Get-Item -Force -LiteralPath $paths[$ordinal]
   $readStage='Acl'
   $acl=Get-Acl -LiteralPath $paths[$ordinal]
   $readStage='Descriptor'
   $raw=[Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(),0)
   $readStage='Owner'
   $owner=if($raw.Owner){$raw.Owner.Value}else{$null}
   $readStage='DaclLimit'
   if($raw.DiscretionaryAcl.Count -gt 256){$readCategory='InvalidArgument';throw 'finite limit'}
   $aces=@()
   foreach($ace in $raw.DiscretionaryAcl) {
    $readStage='AceType'
    $allowed=([int]$ace.AceType -eq 0)
    $readStage='AceFlags'
    $inheritOnly=(([int]$ace.AceFlags -band 8) -ne 0)
    $readStage='AceIdentity'
    $sid=if($ace.SecurityIdentifier){$ace.SecurityIdentifier.Value}else{$null}
    $readStage='AceMask'
    $mask=if($null -ne $ace.AccessMask){([int64]$ace.AccessMask -band 0xffffffffL)}else{0}
    $readStage='AceClassification'
    $aces+=@{allowed=$allowed;inherit_only=$inheritOnly;effective_allowed=($allowed -and -not $inheritOnly);
     identity_class=(Classify $sid $owner);trusted=(Trusted $sid);mutation_mask=($mask -band 0x500d0152)}
   }
   $readStage='DaclShape'
   $dacl=if($null -eq $raw.DiscretionaryAcl){'Absent'}elseif($raw.DiscretionaryAcl.Count -eq 0){'Empty'}else{'Present'}
   $readStage='ItemMetadata'
   $directory=[bool]$item.PSIsContainer
   $reparse=(([int]$item.Attributes -band 0x400) -ne 0)
   $protected=[bool]$acl.AreAccessRulesProtected
   $readStage='OwnerClassification'
   $ownerClass=(Classify $owner $null)
   $ownerTrusted=(Trusted $owner)
   $readStage='Record'
   $record=@{ordinal=$ordinal;role=$roles[$ordinal];status='Observed';directory=$directory;
    reparse=$reparse;owner_class=$ownerClass;
    owner_trusted=$ownerTrusted;dacl=$dacl;dacl_protected=$protected;aces=$aces}
  }
 } catch {
  $category=[string]$_.CategoryInfo.Category
  if($readCategory){$category=$readCategory}
  if($category -notin @('ParserError','InvalidArgument','ObjectNotFound','PermissionDenied','InvalidOperation','ResourceUnavailable','NotSpecified')){$category='Other'}
  $record=@{ordinal=$ordinal;role=$roles[$ordinal];status='ReadFailed';read_stage=$readStage;error_category=$category}
 }
 $segments+=$record
}
$stage='Serialize'
$json=ConvertTo-Json -Depth 6 -Compress -InputObject @{segments=$segments}
$stage='Complete'
Write-Output $json
} catch {
 $category=[string]$_.CategoryInfo.Category
 if($category -notin @('ParserError','InvalidArgument','ObjectNotFound','PermissionDenied','InvalidOperation','ResourceUnavailable','NotSpecified')){$category='Other'}
 ConvertTo-Json -Compress -InputObject @{status='ScriptFailed';stage=$stage;category=$category}
 exit 1
}
"""


def parse_acl_diagnostics(text):
    """Accept only bounded finite fields; never preserve arbitrary native text."""
    if len(text) > 131072:
        raise ValueError("invalid finite diagnostic")
    value = json.loads(text)
    if type(value) is not dict or set(value) != {"segments"}:
        raise ValueError("invalid finite diagnostic")
    segments = value["segments"]
    if type(segments) is not list or len(segments) != 3:
        raise ValueError("invalid finite diagnostic")
    for ordinal, (segment, role) in enumerate(zip(segments, ["Volume", "Outer", "Private"])):
        base = {"ordinal", "role", "status"}
        if (type(segment) is not dict or type(segment.get("ordinal")) is not int
                or segment["ordinal"] != ordinal or segment.get("role") != role
                or segment.get("status") not in {"Observed", "Missing", "ReadFailed"}):
            raise ValueError("invalid finite diagnostic")
        if segment["status"] == "ReadFailed":
            if (set(segment) != base | {"read_stage", "error_category"}
                    or segment["read_stage"] not in READ_STAGES
                    or segment["error_category"] not in ERROR_CATEGORIES):
                raise ValueError("invalid finite diagnostic")
            continue
        if segment["status"] == "Missing":
            if set(segment) != base:
                raise ValueError("invalid finite diagnostic")
            continue
        if set(segment) != base | {"directory", "reparse", "owner_class", "owner_trusted", "dacl", "dacl_protected", "aces"}:
            raise ValueError("invalid finite diagnostic")
        if (segment["owner_class"] not in IDENTITY_CLASSES or segment["dacl"] not in {"Absent", "Empty", "Present"}
                or any(type(segment[key]) is not bool for key in ["directory", "reparse", "owner_trusted", "dacl_protected"])
                or type(segment["aces"]) is not list or len(segment["aces"]) > 256):
            raise ValueError("invalid finite diagnostic")
        if (segment["dacl"] == "Present") != bool(segment["aces"]):
            raise ValueError("invalid finite diagnostic")
        for ace in segment["aces"]:
            if (type(ace) is not dict or set(ace) != {"allowed", "inherit_only", "effective_allowed", "identity_class", "trusted", "mutation_mask"}
                    or ace["identity_class"] not in IDENTITY_CLASSES
                    or any(type(ace[key]) is not bool for key in ["allowed", "inherit_only", "effective_allowed", "trusted"])
                    or ace["effective_allowed"] != (ace["allowed"] and not ace["inherit_only"])
                    or type(ace["mutation_mask"]) is not int or ace["mutation_mask"] < 0
                    or ace["mutation_mask"] & ~MUTATION_MASK):
                raise ValueError("invalid finite diagnostic")
    return value


def fixture_failure_phase(stderr):
    """Classify only static production admission guards, without native payloads."""
    for message, phase in [
        ("namespace permits foreign mutation", "NamespaceForeignMutation"),
        ("namespace owner is unsafe", "NamespaceOwnerUnsafe"),
        ("namespace owner is absent", "NamespaceOwnerAbsent"),
        ("namespace dacl is absent", "NamespaceDaclAbsent"),
        ("namespace dacl is empty", "NamespaceDaclEmpty"),
        ("unsupported namespace acl", "NamespaceAclUnsupported"),
        ("namespace directory type is unsafe", "NamespaceTypeUnsafe"),
    ]:
        if message in stderr:
            return phase
    return "OtherSetupFailure"


def parse_script_failure(text):
    """Validate the child's exact finite failure envelope, without error text."""
    if len(text) > 131072:
        raise ValueError("invalid finite diagnostic")
    value = json.loads(text)
    if (type(value) is not dict or set(value) != {"status", "stage", "category"}
            or value["status"] != "ScriptFailed" or value["stage"] not in SCRIPT_STAGES
            or value["category"] not in ERROR_CATEGORIES):
        raise ValueError("invalid finite diagnostic")
    return value["stage"], value["category"]


def parser_error_category(stderr):
    """Classify only PowerShell's exact CategoryInfo field, never nearby text.

    This is contextual metadata for a nonzero exit, not ACL admission evidence.
    """
    return "ParserError" if re.search(r"(?m)^\s*\+?\s*CategoryInfo\s*:\s*ParserError\s*(?::|$)", stderr) else "Unknown"


def bounded_integer(value, lower, upper):
    return type(value) is int and lower <= value <= upper


def validate_diagnostic_record(record):
    """Require an exact finite discriminated union before writing metadata."""
    common = {"source_phase", "diagnostic_version", "executable_resolution", "diagnostic_status"}
    phases = {"NamespaceForeignMutation", "NamespaceOwnerUnsafe", "NamespaceOwnerAbsent",
              "NamespaceDaclAbsent", "NamespaceDaclEmpty", "NamespaceAclUnsupported",
              "NamespaceTypeUnsafe", "OtherSetupFailure"}
    if (type(record) is not dict or not common <= record.keys()
            or type(record["diagnostic_version"]) is not int or record["diagnostic_version"] != 1
            or record["source_phase"] not in phases
            or record["executable_resolution"] not in {"Resolved", "Missing", "ResolutionFailed"}):
        raise ValueError("invalid finite diagnostic")
    status = record["diagnostic_status"]
    fields = set(record) - common
    if status == "Observed" and fields == {"segments"}:
        parse_acl_diagnostics(json.dumps({"segments": record["segments"]}))
    elif status == "LaunchFailed" and fields in ({"launch_error_class"}, {"launch_error_class", "native_os_error"}):
        if (record["launch_error_class"] not in {"FileNotFound", "PermissionDenied", "OtherOSError"}
                or ("native_os_error" in record and not bounded_integer(record["native_os_error"], 0, 2**32 - 1))):
            raise ValueError("invalid finite diagnostic")
    elif status == "TimedOut" and fields == {"timeout_seconds"}:
        if type(record["timeout_seconds"]) is not int or record["timeout_seconds"] != 30:
            raise ValueError("invalid finite diagnostic")
    elif status == "ProcessFailed" and fields == {"return_code", "failure_stage", "error_category"}:
        if (not bounded_integer(record["return_code"], -(2**31), 2**32 - 1) or record["return_code"] == 0
                or record["failure_stage"] not in SCRIPT_STAGES | {"Unknown"}
                or record["error_category"] not in ERROR_CATEGORIES | {"Unknown"}):
            raise ValueError("invalid finite diagnostic")
    elif status != "ParserRejected" or fields:
        raise ValueError("invalid finite diagnostic")
    return record


def capture_windows_setup_diagnostics(outer, private, stderr, evidence):
    """Read the three fixture ancestors after failed setup, before any user probe.

    This is diagnostic metadata, not native ACL admission or a denial receipt.
    Subprocess failure or unexpected output produces only a finite status.
    """
    try:
        resolution = "Resolved" if shutil.which("powershell") else "Missing"
    except OSError:
        resolution = "ResolutionFailed"
    record = {"source_phase": fixture_failure_phase(stderr), "diagnostic_version": 1,
              "executable_resolution": resolution, "diagnostic_status": "ParserRejected"}
    try:
        result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ACL_DIAGNOSTIC_SCRIPT],
                                env={**os.environ, "TSD_ACL_DIAGNOSTIC_PATHS": json.dumps([outer.anchor, str(outer), str(private)])},
                                capture_output=True, text=True, timeout=30)
        if not bounded_integer(result.returncode, -(2**31), 2**32 - 1):
            pass
        elif result.returncode == 0:
            try:
                record.update(parse_acl_diagnostics(result.stdout))
                record["diagnostic_status"] = "Observed"
            except (ValueError, TypeError, KeyError):
                pass
        else:
            try:
                stage, category = parse_script_failure(result.stdout)
            except (ValueError, TypeError, KeyError):
                stage, category = "Unknown", parser_error_category(result.stderr)
            record.update(diagnostic_status="ProcessFailed", return_code=result.returncode,
                          failure_stage=stage, error_category=category)
    except subprocess.TimeoutExpired:
        record.update(diagnostic_status="TimedOut", timeout_seconds=30)
    except OSError as error:
        kind = "FileNotFound" if isinstance(error, FileNotFoundError) else "PermissionDenied" if isinstance(error, PermissionError) else "OtherOSError"
        record.update(diagnostic_status="LaunchFailed", launch_error_class=kind)
        number = getattr(error, "winerror", None)
        if number is None:
            number = error.errno
        if bounded_integer(number, 0, 2**32 - 1):
            record["native_os_error"] = number
    validate_diagnostic_record(record)
    (evidence / "fixture-acl-diagnostics.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def check_windows_diagnostic_launcher(evidence):
    """Execute the actual diagnostic launcher on owned disposable directories.

    This neither admits a production namespace nor runs the separate-user probe.
    No account is created and no ACL is modified.
    """
    if os.name != "nt":
        raise RuntimeError("diagnostic launcher regression requires native Windows")
    with tempfile.TemporaryDirectory(prefix="tsd-acl-launcher-") as name:
        outer = Path(name)
        private = outer / "private"
        private.mkdir()
        record = capture_windows_setup_diagnostics(outer, private, "", evidence)
        if record["diagnostic_status"] != "Observed" or any(segment["status"] != "Observed" for segment in record["segments"]):
            # Only already validated finite metadata reaches the console.
            print(json.dumps(record, sort_keys=True))
            raise RuntimeError("native diagnostic launcher did not observe three fixture segments")
        print("WINDOWS_DIAGNOSTIC_LAUNCHER_THREE_SEGMENTS_OBSERVED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--check-windows-diagnostic-launcher", action="store_true")
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    evidence = (args.evidence_dir or ROOT / "target") / ("native-user-denial-" + stamp)
    evidence.mkdir(parents=True)
    capture_provenance(ROOT, evidence)
    if args.check_windows_diagnostic_launcher:
        check_windows_diagnostic_launcher(evidence)
        return
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
            if windows:
                capture_windows_setup_diagnostics(outer, private, result.stderr, evidence)
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
