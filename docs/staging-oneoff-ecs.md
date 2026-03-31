# Staging One-Off ECS Run

This runbook prepares a one-off CDSync task in AWS staging using the existing staging worker network path.

## Why This Path

The local tunnel path is awkward for logical replication because the replication client validates the RDS hostname during TLS setup. Running from inside `cluster-staging` avoids the tunnel and connects directly to the staging writer with SSL.

## Current AWS Inputs

- ECS cluster: `cluster-staging`
- Subnets:
  - `subnet-0847f8d9415013ba2`
  - `subnet-0a790db993d728821`
- Security group: `sg-06d149ded90962461` (`staging-services`)
- Staging writer:
  - `staging-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com`
- BigQuery project: `nora-461013`
- Temporary dataset: `cdsync_e2e_real`
- Publication: `cdsync_staging_pub`
- CDSync DB user: `cdsync_staging`

## First-Run Shape

The task definition template is:

- [cdsync-staging-oneoff-taskdef.template.json](/Users/mazdak/Code/cdsync/deploy/ecs/cdsync-staging-oneoff-taskdef.template.json)

The helper script is:

- [run-staging-oneoff.sh](/Users/mazdak/Code/cdsync/deploy/ecs/run-staging-oneoff.sh)

The task:

- writes a temporary config file inside the container
- validates the staging publication
- runs a one-shot CDC sync
- emits a short run report

## Required Inputs

Before running the helper, create or choose:

1. a pushed container image URI
2. an SSM parameter ARN containing the CDSync DB password
3. an SSM parameter ARN containing the base64-encoded GCP service-account JSON

Recommended parameter names:

- `/com/getresq/staging/cdsync/database_password`
- `/com/getresq/staging/cdsync/gcp_key_b64`

## Current Staging Spike Values

Current image URI:

```text
176982589840.dkr.ecr.us-east-1.amazonaws.com/fullstack-image:cdsync-staging-spike-20260330-63671d0-dirty
```

Current image digest:

```text
sha256:e3a52f03f9a41c8ea7fb8e2228a649bad5af20db9c65a4c494037532a0247ddc
```

Current SSM parameter ARNs:

```text
arn:aws:ssm:us-east-1:176982589840:parameter/com/getresq/staging/cdsync/database_password
arn:aws:ssm:us-east-1:176982589840:parameter/com/getresq/staging/cdsync/gcp_key_b64
```

Temporary BigQuery target:

```text
project: nora-461013
dataset: cdsync_e2e_real
```

Staging GCS batch-load bucket:

```text
gs://nora-461013-cdsync-staging-loads
```

Current bucket settings:

- location: `US`
- uniform bucket-level access: enabled
- lifecycle delete rule: objects older than 1 day

Current one-off write mode:

- `storage_write_enabled = false`
- `batch_load_bucket = "nora-461013-cdsync-staging-loads"`
- `batch_load_prefix = "staging-app"`

This means the next one-off run will prefer:

- NDJSON upload to GCS
- BigQuery load jobs from GCS

instead of the Storage Write API for bulk append batches.

Notes:

- the DB password itself is stored only in SSM, not in git
- the GCP service-account JSON is stored only in SSM, not in git
- the `cdsync_e2e_real` dataset is a temporary reuse and should be replaced by a dedicated staging shadow dataset later

## Build And Push

An existing ECR repo already exists:

- `176982589840.dkr.ecr.us-east-1.amazonaws.com/fullstack-image`

Example:

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=176982589840
export IMAGE_TAG="cdsync-staging-$(git rev-parse --short HEAD)"
export IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/fullstack-image:${IMAGE_TAG}"

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker build --platform linux/amd64 -f docker/Dockerfile.runner -t "${IMAGE_URI}" .
docker push "${IMAGE_URI}"
```

## Run The Task

```bash
chmod +x ./deploy/ecs/run-staging-oneoff.sh

./deploy/ecs/run-staging-oneoff.sh \
  "${IMAGE_URI}" \
  "arn:aws:ssm:us-east-1:176982589840:parameter/com/getresq/staging/cdsync/database_password" \
  "arn:aws:ssm:us-east-1:176982589840:parameter/com/getresq/staging/cdsync/gcp_key_b64"
```

The script prints the task ARN.

## Monitor Progress

Describe task state:

```bash
aws ecs describe-tasks \
  --region us-east-1 \
  --cluster cluster-staging \
  --tasks <task-arn>
```

Tail logs:

```bash
aws logs tail /ecs/staging-resq-run-task \
  --region us-east-1 \
  --follow \
  --since 1h
```

The task uses `awslogs` with stream prefix `cdsync`, so its container logs should be easy to isolate.

## SSL Expectations

This one-off task uses SSL for both paths:

- normal Postgres queries: `sslmode=require`
- CDC replication stream:
  - `cdc_tls: true`
  - `cdc_tls_ca_path: /etc/ssl/certs/ca-certificates.crt`

Because the task connects directly to the staging RDS hostname from inside the VPC, hostname validation should work normally.

## Temporary Compromises

- BigQuery target is temporarily `cdsync_e2e_real`
- state and stats now live in Postgres schemas instead of local SQLite files
- the CDSync runtime therefore needs schema/table creation privileges for the configured state and stats schemas
- logging goes to CloudWatch first; Datadog sidecars can be added later if needed

## Follow-Up

If the one-off task succeeds, the next step is either:

1. repeat one-off runs while we inspect data quality and source load, or
2. promote to a long-running ECS service with durable state storage
