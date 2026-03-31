#!/usr/bin/env bash
set -euo pipefail

export PGHOST=localhost
export PGPORT=5434
export PGDATABASE="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_NAME --with-decryption --query 'Parameter.Value' --output text)"
export PGUSER="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_USER --with-decryption --query 'Parameter.Value' --output text)"
export PGPASSWORD="$(aws ssm get-parameter --region us-east-1 --name /com/getresq/staging/env/generated/DATABASE_PASSWORD --with-decryption --query 'Parameter.Value' --output text)"

psql "sslmode=require"
