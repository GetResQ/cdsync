#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <connection-id> <output-dir>"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

if [[ -z "${FIVETRAN_API_KEY:-}" || -z "${FIVETRAN_API_SECRET:-}" ]]; then
  echo "set FIVETRAN_API_KEY and FIVETRAN_API_SECRET"
  exit 1
fi

CONNECTION_ID="$1"
OUTPUT_DIR="$2"
BASE_URL="https://api.fivetran.com/v1"

mkdir -p "$OUTPUT_DIR"

curl -sS -u "${FIVETRAN_API_KEY}:${FIVETRAN_API_SECRET}" \
  "${BASE_URL}/connections/${CONNECTION_ID}" \
  > "${OUTPUT_DIR}/connection.json"

curl -sS -u "${FIVETRAN_API_KEY}:${FIVETRAN_API_SECRET}" \
  "${BASE_URL}/connections/${CONNECTION_ID}/schemas" \
  > "${OUTPUT_DIR}/schemas.json"

jq -r '
  .data.schemas
  | to_entries[]
  | .key as $schema_name
  | .value.tables
  | to_entries[]
  | [
      $schema_name,
      .key,
      (.value.enabled | tostring),
      .value.sync_mode,
      ([.value.columns[] | select(.enabled == true)] | length | tostring),
      ([.value.columns[] | select(.enabled == false)] | length | tostring)
    ]
  | @tsv
' "${OUTPUT_DIR}/schemas.json" > "${OUTPUT_DIR}/tables.tsv"

jq -r '
  .data.schemas
  | to_entries[]
  | .key as $schema_name
  | .value.tables
  | to_entries[]
  | select(.value.enabled == true)
  | "[[connections.0.source.tables]]\nname = \"" + $schema_name + "." + .key + "\"\nsoft_delete = " + ((.value.sync_mode == "SOFT_DELETE") | tostring) + "\n"
' "${OUTPUT_DIR}/schemas.json" > "${OUTPUT_DIR}/cdsync_tables.toml"

echo "Wrote:"
echo "  ${OUTPUT_DIR}/connection.json"
echo "  ${OUTPUT_DIR}/schemas.json"
echo "  ${OUTPUT_DIR}/tables.tsv"
echo "  ${OUTPUT_DIR}/cdsync_tables.toml"
