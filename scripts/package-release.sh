#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  cat >&2 <<'EOF'
Usage: scripts/package-release.sh <target-triple> <binary-path> <output-dir>

Example:
  scripts/package-release.sh x86_64-unknown-linux-gnu target/release/cdsync dist
EOF
  exit 2
fi

target_triple="$1"
binary_path="$2"
output_dir="$3"
archive_base="cdsync-${target_triple}"
staging_dir="${output_dir}/${archive_base}"
archive_path="${output_dir}/${archive_base}.tar.gz"
checksum_path="${archive_path}.sha256"

if [[ ! -f "$binary_path" ]]; then
  echo "binary not found: ${binary_path}" >&2
  exit 1
fi

rm -rf "$staging_dir"
mkdir -p "$staging_dir"
install -m 0755 "$binary_path" "${staging_dir}/cdsync"

tar -C "$output_dir" -czf "$archive_path" "$archive_base"
(
  cd "$output_dir"
  sha256sum "${archive_base}.tar.gz" >"$(basename "$checksum_path")"
)

rm -rf "$staging_dir"
