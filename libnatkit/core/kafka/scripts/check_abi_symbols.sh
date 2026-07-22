#!/usr/bin/env bash
#
# check_abi_symbols.sh -- guard the liblibnatkit-kafka C ABI against accidental
# breaking changes, per the append-only versioning policy in
# lib/libnatkit-core/docs/ABI_CONVENTIONS.md. Mirrors the libnatkit-core check.
#
# It extracts the unmangled exported `nat_kafka_*` symbols from a built shared
# library and fails if any symbol in abi/symbols.txt is MISSING (removed/renamed
# = breaking). New symbols are allowed; the run reports them so they can be added
# to the baseline in the same commit.
#
# Usage: check_abi_symbols.sh [path/to/liblibnatkit-kafka.so]
# Falls back to the standard build-tree location relative to the repo root.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
kafka_root="$(cd "${script_dir}/.." && pwd)"
baseline="${kafka_root}/abi/symbols.txt"

lib="${1:-}"
if [[ -z "${lib}" ]]; then
  # kafka_root = <libnatkit>/libnatkit/core/kafka; the build tree is <libnatkit>/build
  libnatkit_root="$(cd "${kafka_root}/../../.." && pwd)"
  lib="${libnatkit_root}/build/libnatkit/core/kafka/src/liblibnatkit-kafka.so"
fi

if [[ ! -f "${lib}" ]]; then
  echo "ERROR: shared library not found: ${lib}" >&2
  echo "Build it first (cmake --build build --target libnatkit-kafka) or pass the path." >&2
  exit 2
fi
if [[ ! -f "${baseline}" ]]; then
  echo "ERROR: baseline not found: ${baseline}" >&2
  exit 2
fi

current="$(nm -D --defined-only "${lib}" \
  | awk '$2=="T"{print $3}' \
  | grep -E '^nat_kafka_' \
  | sort -u)"

expected="$(grep -vE '^\s*(#|$)' "${baseline}" | sort -u)"

missing="$(comm -23 <(printf '%s\n' "${expected}") <(printf '%s\n' "${current}"))"
added="$(comm -13 <(printf '%s\n' "${expected}") <(printf '%s\n' "${current}"))"

status=0
if [[ -n "${missing}" ]]; then
  echo "ABI CHECK FAILED: baseline symbols missing from the library" >&2
  echo "${missing}" | sed 's/^/  - /' >&2
  echo "A removed or renamed exported symbol is a breaking ABI change." >&2
  echo "Ship the change as a new _vN_ symbol instead of editing the old one." >&2
  status=1
fi

if [[ -n "${added}" ]]; then
  echo "Note: new exported symbols not yet in the baseline (append them to abi/symbols.txt):"
  echo "${added}" | sed 's/^/  + /'
fi

if [[ "${status}" -eq 0 ]]; then
  echo "ABI check passed: all $(printf '%s\n' "${expected}" | grep -c .) baseline symbols present."
fi
exit "${status}"
