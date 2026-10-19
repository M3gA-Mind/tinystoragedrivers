#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <package> <allowlist>" >&2
  exit 2
fi

package=$1
allowlist=$2
if [[ ! -f "$allowlist" ]]; then
  echo "dependency allowlist does not exist: $allowlist" >&2
  exit 2
fi

# Capture cargo's exit status before parsing: a failed tree command must never
# become an empty, successful dependency set. Compare package names only;
# checkout paths and versions are not part of the dependency policy.
if ! tree=$(cargo tree -p "$package" --all-features -e normal,build --prefix none); then
  echo "could not read the forward dependency tree for $package" >&2
  exit 1
fi
if [[ -z "$tree" ]]; then
  echo "cargo returned an empty dependency tree for $package" >&2
  exit 1
fi

export LC_ALL=C
actual=$(printf '%s\n' "$tree" | awk 'NF { print $1 }' | sort -u)
allowed=$(awk 'NF && $1 !~ /^#/ { print $1 }' "$allowlist" | sort -u)
if [[ -z "$actual" || -z "$allowed" ]]; then
  echo "dependency tree and allowlist must both contain package names" >&2
  exit 1
fi
if ! grep -Fxq "$package" <<< "$actual"; then
  echo "cargo tree for $package did not include its root package" >&2
  exit 1
fi

unreviewed=$(comm -23 <(printf '%s\n' "$actual") <(printf '%s\n' "$allowed"))
stale=$(comm -13 <(printf '%s\n' "$actual") <(printf '%s\n' "$allowed"))
if [[ -n "$unreviewed" || -n "$stale" ]]; then
  if [[ -n "$unreviewed" ]]; then
    echo "$package has unreviewed normal/build dependencies:" >&2
    printf '%s\n' "$unreviewed" >&2
  fi
  if [[ -n "$stale" ]]; then
    echo "$package allowlist names packages absent from its normal/build tree:" >&2
    printf '%s\n' "$stale" >&2
  fi
  echo "Review the exact cargo tree and update the allowlist with the dependency change." >&2
  exit 1
fi
