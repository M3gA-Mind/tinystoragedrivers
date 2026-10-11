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
import stat
import subprocess
import tempfile

import sys
sys.dont_write_bytecode = True
from native_provenance import capture_provenance

ROOT = Path(__file__).resolve().parents[2]
TEST = "file::tests::published_objects_are_ready_for_a_separate_user_access_probe"
MUTATION_MASK = 0x500D0152
IDENTITY_CLASSES = {"Current", "System", "Admins", "TrustedInstaller", "Everyone", "AuthUsers", "BuiltinUsers", "Owner", "Other"}
EXCEPTION_CLASSES = {"CommandNotFound", "ItemNotFound", "DriveNotFound", "ProviderNotFound",
                     "ActionPreferenceStop", "UnauthorizedAccess", "ParameterBinding", "Other"}
READ_STAGES = {"Existence", "Item", "Acl", "Descriptor", "Owner", "DaclLimit",
               "AceType", "AceFlags", "AceIdentity", "AceMask", "AceClassification",
               "DaclShape", "ItemMetadata", "OwnerClassification", "Record"}
SCRIPT_STAGES = {"Start", "Identity", "Paths", "Segments", "Serialize", "Complete"}
ERROR_CATEGORIES = {"ParserError", "InvalidArgument", "ObjectNotFound", "PermissionDenied",
                    "InvalidOperation", "ResourceUnavailable", "NotSpecified", "Other"}

# Paths enter only through the child environment and Get-Item's literal argument.
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
# Compare known type names internally; unknown names never leave the child.
function ExceptionClass($exception) {
 try {
  switch($exception.GetType().FullName) {
   'System.Management.Automation.CommandNotFoundException' {return 'CommandNotFound'}
   'System.Management.Automation.ItemNotFoundException' {return 'ItemNotFound'}
   'System.Management.Automation.DriveNotFoundException' {return 'DriveNotFound'}
   'System.Management.Automation.ProviderNotFoundException' {return 'ProviderNotFound'}
   'System.Management.Automation.ActionPreferenceStopException' {return 'ActionPreferenceStop'}
   'System.UnauthorizedAccessException' {return 'UnauthorizedAccess'}
   'System.Management.Automation.ParameterBindingException' {return 'ParameterBinding'}
  }
 } catch {}
 return 'Other'
}
function InnerExceptionClasses($exception) {
 $classes=@()
 for($depth=0;$depth -lt 2;$depth++) {
  try { $exception=$exception.InnerException } catch { break }
  if($null -eq $exception){break}
  $classes+=ExceptionClass $exception
 }
 return ,$classes
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
   if($item -isnot [IO.FileInfo] -and $item -isnot [IO.DirectoryInfo]) {
    $readCategory='InvalidArgument';throw 'filesystem object required'
   }
   # Owner and discretionary ACL only, matching the native reader's sections.
   $sections=[Security.AccessControl.AccessControlSections]::Owner -bor [Security.AccessControl.AccessControlSections]::Access
   $acl=$item.GetAccessControl($sections)
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
  $exception=$_.Exception
  $record=@{ordinal=$ordinal;role=$roles[$ordinal];status='ReadFailed';read_stage=$readStage;error_category=$category;
   exception_class=(ExceptionClass $exception);inner_exception_classes=(InnerExceptionClasses $exception)}
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
            if (set(segment) != base | {"read_stage", "error_category", "exception_class", "inner_exception_classes"}
                    or type(segment["read_stage"]) is not str or segment["read_stage"] not in READ_STAGES
                    or type(segment["error_category"]) is not str or segment["error_category"] not in ERROR_CATEGORIES
                    or type(segment["exception_class"]) is not str or segment["exception_class"] not in EXCEPTION_CLASSES
                    or type(segment["inner_exception_classes"]) is not list
                    or len(segment["inner_exception_classes"]) > 2
                    or any(type(name) is not str or name not in EXCEPTION_CLASSES
                           for name in segment["inner_exception_classes"])):
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


# Reconstruct only the freshly created, empty, owned disposable outer. Never
# edit an ancestor, public sibling, or production-private descriptor here.
OUTER_ACL_SCRIPT = r"""$ErrorActionPreference='Stop'
try {
 $path=ConvertFrom-Json -InputObject $env:TSD_OUTER_FIXTURE_PATH
 if($path -isnot [string] -or $path.Length -eq 0){throw 'invalid fixture'}
 $item=Get-Item -Force -LiteralPath $path
 if($item -isnot [IO.DirectoryInfo] -or ([int]$item.Attributes -band 0x400) -ne 0){throw 'invalid fixture'}
 if($item.Name -notmatch '^tsd-denial-[a-zA-Z0-9_-]+$' -or
    $item.Parent.FullName -ne $item.Root.FullName -or
    @($item.GetFileSystemInfos()).Count -ne 0){throw 'invalid fixture'}
 $current=[Security.Principal.WindowsIdentity]::GetCurrent().User
 $system=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::LocalSystemSid,$null)
 $admins=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,$null)
 $everyone=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::WorldSid,$null)
 $owner=$item.GetAccessControl([Security.AccessControl.AccessControlSections]::Owner).GetOwner([Security.Principal.SecurityIdentifier])
 if($owner -ne $current -and $owner -ne $admins){throw 'invalid fixture owner'}
 $acl=[Security.AccessControl.DirectorySecurity]::new()
 $acl.SetOwner($current)
 $acl.SetAccessRuleProtection($true,$false)
 $inherit=[Security.AccessControl.InheritanceFlags]::ObjectInherit -bor [Security.AccessControl.InheritanceFlags]::ContainerInherit
 $propagate=[Security.AccessControl.PropagationFlags]::None
 $allow=[Security.AccessControl.AccessControlType]::Allow
 foreach($identity in @($current,$system,$admins)) {
  $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($identity,[Security.AccessControl.FileSystemRights]::FullControl,$inherit,$propagate,$allow))
 }
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($everyone,[Security.AccessControl.FileSystemRights]::ReadAndExecute,$inherit,$propagate,$allow))
 $item.SetAccessControl($acl)
 Write-Output 'OUTER_ACL_RECONSTRUCTED'
} catch { exit 1 }
"""


def validate_windows_outer_acl(segment):
    """Require the actual finite readback to match the disposable outer ACL."""
    expected = {"Current": (True, 0xD0152), "System": (True, 0xD0152),
                "Admins": (True, 0xD0152), "Everyone": (False, 0)}
    if (segment.get("status") != "Observed" or segment.get("directory") is not True
            or segment.get("reparse") is not False or segment.get("owner_class") != "Current"
            or segment.get("owner_trusted") is not True or segment.get("dacl") != "Present"
            or segment.get("dacl_protected") is not True or len(segment.get("aces", [])) != 4):
        raise RuntimeError("disposable outer ACL readback failed")
    remaining = dict(expected)
    for ace in segment["aces"]:
        identity = ace["identity_class"]
        if (identity not in remaining or not ace["allowed"] or ace["inherit_only"]
                or not ace["effective_allowed"]
                or (ace["trusted"], ace["mutation_mask"]) != remaining.pop(identity)):
            raise RuntimeError("disposable outer ACL readback failed")


def configure_windows_outer(outer, evidence):
    """Replace a just-created fixture descriptor, then verify native readback.

    Callers own this empty mkdtemp directory. Paths are environment JSON data;
    native failure streams and exception payloads are always discarded.
    """
    try:
        result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", OUTER_ACL_SCRIPT],
                                env={**os.environ, "TSD_OUTER_FIXTURE_PATH": json.dumps(str(outer))},
                                capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("disposable outer ACL reconstruction failed") from None
    if result.returncode != 0 or result.stdout.strip() != "OUTER_ACL_RECONSTRUCTED" or result.stderr.strip():
        raise RuntimeError("disposable outer ACL reconstruction failed")
    record = capture_windows_setup_diagnostics(outer, outer / "private", "", evidence)
    if record["diagnostic_status"] != "Observed":
        raise RuntimeError("disposable outer ACL readback failed")
    validate_windows_outer_acl(record["segments"][1])


PROBE_FILE_ACL_SCRIPT = r"""$ErrorActionPreference='Stop'
try {
 $path=ConvertFrom-Json -InputObject $env:TSD_PROBE_SCRIPT_PATH
 if($path -isnot [string]){throw 'invalid script'}
 $item=Get-Item -Force -LiteralPath $path
 if($item -isnot [IO.FileInfo] -or ([int]$item.Attributes -band 0x400) -ne 0 -or
    $item.Name -ne 'denial-probe.ps1' -or $item.Directory.Name -notmatch '^tsd-denial-[a-zA-Z0-9_-]+$' -or
    $item.Directory.Parent.FullName -ne $item.Directory.Root.FullName){throw 'invalid script'}
 $current=[Security.Principal.WindowsIdentity]::GetCurrent().User
 $owner=$item.GetAccessControl([Security.AccessControl.AccessControlSections]::Owner).GetOwner([Security.Principal.SecurityIdentifier])
 $admins=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,$null)
 if($owner -ne $current -and $owner -ne $admins){throw 'invalid owner'}
 $acl=[Security.AccessControl.FileSecurity]::new()
 $acl.SetOwner($current);$acl.SetAccessRuleProtection($true,$false)
 $system=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::LocalSystemSid,$null)
 $everyone=[Security.Principal.SecurityIdentifier]::new([Security.Principal.WellKnownSidType]::WorldSid,$null)
 foreach($identity in @($current,$system,$admins)) {
  $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($identity,[Security.AccessControl.FileSystemRights]::FullControl,[Security.AccessControl.AccessControlType]::Allow))
 }
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($everyone,[Security.AccessControl.FileSystemRights]::ReadAndExecute,[Security.AccessControl.AccessControlType]::Allow))
 $item.SetAccessControl($acl)
 Write-Output 'PROBE_SCRIPT_PROTECTED'
} catch {exit 1}
"""


def validate_probe_file_identity(path, identity=None):
    """Reject links/reparse objects and any changed create-new file identity."""
    try:
        info = path.lstat()
    except OSError:
        raise RuntimeError("unsafe disposable probe script") from None
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or getattr(info, "st_file_attributes", 0) & 0x400
            or (identity is not None and (info.st_dev, info.st_ino) != identity)):
        raise RuntimeError("unsafe disposable probe script")
    return info.st_dev, info.st_ino


def validate_probe_file_acl(segment):
    if segment.get("directory") is not False:
        raise RuntimeError("unsafe disposable probe script ACL")
    validate_windows_outer_acl({**segment, "directory": True})


def create_windows_probe_file(outer, child, evidence):
    """Create an exclusive protected read-only-foreign script in admitted outer."""
    path = outer / "denial-probe.ps1"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
    except OSError:
        raise RuntimeError("exclusive disposable probe script creation failed") from None
    with os.fdopen(descriptor, "w", encoding="utf-8-sig", newline="") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or getattr(info, "st_file_attributes", 0) & 0x400:
            raise RuntimeError("unsafe disposable probe script")
        identity = info.st_dev, info.st_ino
        handle.write(child)
    validate_probe_file_identity(path, identity)
    try:
        result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", PROBE_FILE_ACL_SCRIPT],
                                env={**os.environ, "TSD_PROBE_SCRIPT_PATH": json.dumps(str(path))},
                                capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("probe script protection failed") from None
    if result.returncode != 0 or result.stdout.strip() != "PROBE_SCRIPT_PROTECTED" or result.stderr.strip():
        raise RuntimeError("probe script protection failed")
    record = capture_windows_setup_diagnostics(outer, path, "", evidence)
    if record["diagnostic_status"] != "Observed":
        raise RuntimeError("probe script ACL readback failed")
    validate_probe_file_acl(record["segments"][2])
    (evidence / "probe-script-acl.json").write_text(json.dumps(record, indent=2) + "\n")
    validate_probe_file_identity(path, identity)
    if path.read_bytes() != b"\xef\xbb\xbf" + child.encode("utf-8"):
        raise RuntimeError("disposable probe script changed")
    return path, identity


def parse_launch_diagnostic(text):
    """Accept only bounded controlled-launch outcomes, never native payloads."""
    if len(text) > 8192:
        raise ValueError("invalid finite launch diagnostic")
    record = json.loads(text)
    fields = {"status", "phase", "oversized_outcome", "oversized_length", "short_length",
              "short_within_bound", "child_exit", "cleanup_complete"}
    if (type(record) is not dict or set(record) != fields
            or record["status"] not in {"Passed", "Failed"}
            or record["phase"] not in {"Password", "Account", "Group", "Credential", "Oversized", "Short", "Child", "Complete"}
            or record["oversized_outcome"] not in {"NotRun", "ParameterIncorrect", "UnexpectedFailure", "UnexpectedSuccess"}
            or any(not bounded_integer(record[key], 0, 131072) for key in ["oversized_length", "short_length"])
            or type(record["short_within_bound"]) is not bool
            or type(record["cleanup_complete"]) is not bool
            or (record["child_exit"] is not None and not bounded_integer(record["child_exit"], -(2**31), 2**32-1))):
        raise ValueError("invalid finite launch diagnostic")
    if record["short_within_bound"] != (0 < record["short_length"] <= 1024):
        raise ValueError("invalid finite launch diagnostic")
    if record["status"] == "Passed" and (record["phase"] != "Complete" or record["oversized_outcome"] != "ParameterIncorrect"
            or record["oversized_length"] <= 1024 or not record["short_within_bound"]
            or record["child_exit"] != 0 or not record["cleanup_complete"]):
        raise ValueError("invalid finite launch diagnostic")
    return record


# Shared by the actual account parent and account-free native regressions.
# Never emit the generated password or convert the SecureString back to text.
WINDOWS_PASSWORD_CONSTRUCTION = """foreach($character in ([Guid]::NewGuid().ToString('N')+'aA!7').ToCharArray()) {
  $password.AppendChar($character)
 }
 $password.MakeReadOnly()"""
WINDOWS_PASSWORD_CLEANUP = """try {
  if($created){Remove-LocalUser -Name $name}
 } finally {
  $password.Dispose()
 }"""


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
            configure_windows_outer(outer, evidence)
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
            script_path, script_identity = create_windows_probe_file(outer, child, evidence)
            encoded = base64.b64encode(child.encode("utf-16le")).decode()
            # Both launches use this same account/credential/settings. The old
            # oversized command must fail before the short-file probe is accepted.
            script = """$ErrorActionPreference='Stop'
$status='Failed';$phase='Password';$oldOutcome='NotRun';$oldLength=0;$shortLength=0
$within=$false;$childExit=$null;$cleanup=$false
try {
 $name='tsd'+[Guid]::NewGuid().ToString('N').Substring(0,12)
 $password=[System.Security.SecureString]::new()
 $created=$false
 try {
  PASSWORD_CONSTRUCTION
  $phase='Account'
  New-LocalUser -Name $name -Password $password | Out-Null; $created=$true
  $phase='Group'
  Add-LocalGroupMember -Group (Get-LocalGroup -SID 'S-1-5-32-545') -Member $name
  $phase='Credential'
  $credential=[PSCredential]::new(($env:COMPUTERNAME+'\\'+$name),$password)
  $executable="$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"
  $oldArgs=@('-NoProfile','-EncodedCommand','ENCODED')
  $shortArgs=@('-NoProfile','-File',SCRIPTFILE)
  $oldLength=('"'+$executable+'" '+($oldArgs -join ' ')).Length
  $shortLength=('"'+$executable+'" '+($shortArgs -join ' ')).Length
  $within=($shortLength -gt 0 -and $shortLength -le 1024)
  if(-not $within -or $oldLength -le 1024){throw 'command bound failed'}
  $phase='Oversized'
  try {
   $p=Start-Process -FilePath $executable -Credential $credential -ArgumentList $oldArgs -Wait -PassThru -RedirectStandardOutput OUT -RedirectStandardError ERR
   $oldOutcome='UnexpectedSuccess'
  } catch {
   $oldOutcome='UnexpectedFailure'
   $known=([string]$_.CategoryInfo.Category -eq 'InvalidOperation')
   $parameter=$false;$exception=$_.Exception
   for($depth=0;$depth -lt 4 -and $null -ne $exception;$depth++) {
    if($exception.Message -like '*parameter is incorrect*'){$parameter=$true}
    $exception=$exception.InnerException
   }
   if($known -and $parameter){$oldOutcome='ParameterIncorrect'}
  }
  if($oldOutcome -ne 'ParameterIncorrect'){throw 'oversized launch control failed'}
  $phase='Short'
  $p=Start-Process -FilePath $executable -Credential $credential -ArgumentList $shortArgs -Wait -PassThru -RedirectStandardOutput OUT -RedirectStandardError ERR
  $phase='Child';$childExit=$p.ExitCode
  if($childExit -ne 0){throw 'child probe failed'}
  if([IO.File]::ReadAllText(OUT).Trim() -ne 'ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED'){throw 'child receipt absent'}
 } finally { PASSWORD_CLEANUP }
 $cleanup=$true;$phase='Complete';$status='Passed'
} catch {}
ConvertTo-Json -Compress -InputObject @{status=$status;phase=$phase;oversized_outcome=$oldOutcome;
 oversized_length=$oldLength;short_length=$shortLength;short_within_bound=$within;child_exit=$childExit;cleanup_complete=$cleanup}
if($status -ne 'Passed'){exit 1}
""".replace("PASSWORD_CONSTRUCTION", WINDOWS_PASSWORD_CONSTRUCTION).replace("PASSWORD_CLEANUP", WINDOWS_PASSWORD_CLEANUP).replace("ENCODED", encoded).replace("SCRIPTFILE", literal('"' + str(script_path) + '"')).replace("OUT", literal(str(outer / "child.stdout"))).replace("ERR", literal(str(outer / "child.stderr")))
            result = subprocess.run(["powershell", "-NoProfile", "-Command", script], capture_output=True, text=True)
            # Redirected native streams are private disposable fixture data, never
            # upload artifacts. Delete them before any assertion or failure report.
            try:
                for raw in [outer / "child.stdout", outer / "child.stderr"]:
                    raw.unlink(missing_ok=True)
            except OSError:
                raise RuntimeError("private probe stream cleanup failed") from None
            try:
                launch = parse_launch_diagnostic(result.stdout)
            except (ValueError, TypeError, KeyError):
                (evidence / "controlled-launch.json").write_text('{"diagnostic_status":"ParserRejected"}\n')
                raise RuntimeError("controlled native launch diagnostic rejected") from None
            (evidence / "controlled-launch.json").write_text(json.dumps(launch, indent=2) + "\n")
            validate_probe_file_identity(script_path, script_identity)
            if launch["status"] != "Passed" or result.returncode != 0 or result.stderr.strip():
                raise RuntimeError("controlled separate-user probe failed")
            result = subprocess.CompletedProcess([], 0, "ACTUAL_STANDARD_USER_SIXTEEN_OPERATIONS_DENIED\n", "")
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
