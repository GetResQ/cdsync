#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <cdsync_password>"
  exit 1
fi

CDSYNC_PASSWORD="$1"
TEMP_SQL="$(mktemp)"
trap 'rm -f "$TEMP_SQL"' EXIT

export PGHOST=localhost
export PGPORT=5434
export PGDATABASE="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_NAME --with-decryption --query 'Parameter.Value' --output text)"
export PGUSER="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_USER --with-decryption --query 'Parameter.Value' --output text)"
export PGPASSWORD="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_PASSWORD --with-decryption --query 'Parameter.Value' --output text)"

sed "s/__CDSYNC_PASSWORD__/${CDSYNC_PASSWORD}/g" \
  /Users/mazdak/Code/cdsync/sql/staging_cdc_setup.sql > "$TEMP_SQL"

psql "sslmode=require" -f "$TEMP_SQL"
