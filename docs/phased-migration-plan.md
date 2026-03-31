# CDSync Phased Migration Plan

This document captures the staged plan for replacing Fivetran with CDSync safely.

## Goal

Replace Fivetran in controlled phases while preserving:

- correctness
- resumability
- operational visibility
- rollback options

The immediate target is the staging environment. Production cutover comes later.

## Guiding Principles

- Start with the smallest useful blast radius.
- Prove CDC on PostgreSQL before deploying a long-running service.
- Use a shadow BigQuery dataset first.
- Do not compare raw CDSync tables to Fivetran tables blindly; compare normalized semantics.
- Keep Fivetran as the control path until CDSync has passed staging and parallel-run acceptance.

## Phase 0: Preconditions

Before any CDSync staging rollout:

1. Confirm the staging PostgreSQL engine type.
   - If staging is Amazon RDS for PostgreSQL, read-replica CDC may be possible.
   - If staging is Aurora PostgreSQL, assume reader-based logical decoding is not available and use the writer.
2. Confirm PostgreSQL logical replication settings on staging.
3. Decide the first pilot table set.
4. Create a separate BigQuery shadow dataset for CDSync.

## Phase 1: Hook Up CDC To Staging PostgreSQL

Start with CDC first, before deploying CDSync as an always-on ECS service.

### Initial Recommendation

Use the staging writer for the first CDC hookup unless replica-side logical decoding is explicitly validated.

Reason:

- it is the least ambiguous path
- it removes replica-specific uncertainty from the first validation pass
- it lets us prove CDSync CDC behavior before optimizing source placement

### Staging PostgreSQL Checks

Run these against the staging primary:

```sql
show rds.logical_replication;
show wal_level;
show max_replication_slots;
show max_wal_senders;
select version();
```

If testing a staging read replica later, also run:

```sql
select pg_is_in_recovery();
show rds.logical_replication;
show wal_level;
select * from pg_publication;
```

### Create CDSync Database Access

Create a dedicated CDSync database user with:

- `LOGIN`
- `REPLICATION`
- `USAGE` on the relevant schema
- `SELECT` on the pilot tables

### Create A Dedicated Publication

Start with a small pilot set, for example:

```sql
create publication cdsync_staging_pub
for table public.accounts, public.orders, public.work_orders;
```

Avoid row filters in the first pass unless there is a strong reason to add them immediately.

### Pilot Table Criteria

Choose 3 to 5 tables that are:

- primary-keyed
- actively updated
- understandable by humans
- representative of inserts and updates

Include at least one table with deletes if delete semantics matter.

Avoid in the first phase:

- no-primary-key tables
- composite-primary-key tables if not needed yet
- unusually large or awkward partitioned tables
- the most complex schemas in the estate

## Phase 2: First CDSync Validation Against Shadow BigQuery

Create a dedicated staging shadow dataset, for example:

- `cdsync_staging_shadow`

Do not write into the Fivetran-managed dataset yet.

### Configure CDSync

Use one staging config with:

- `cdc: true`
- a dedicated publication such as `cdsync_staging_pub`
- a narrow table list or include pattern
- a distinct BigQuery dataset
- durable local state/stats paths

### First Commands

```bash
cdsync validate --config ./config.staging.yaml --connection staging_app --verbose
cdsync sync --config ./config.staging.yaml --connection staging_app --incremental --schema-diff
cdsync reconcile --config ./config.staging.yaml --connection staging_app
cdsync report --config ./config.staging.yaml --connection staging_app --limit 20
```

### What Must Be Proven

- initial snapshot works
- incremental CDC continues after snapshot
- checkpoints/LSNs persist
- reruns do not replay from zero
- reconcile is clean enough to explain any mismatch
- logs and run reports are sufficient to explain failures

## Phase 3: Failure And Resume Testing In Staging

Before deploying a service, explicitly test failure behavior.

### Required Tests

1. Stop CDSync during or after snapshot, then restart.
2. Stop CDSync during CDC follow mode, then restart.
3. Introduce controlled source mutations while CDSync is down, then restart.
4. Force a BigQuery-side failure and verify:
   - the failure is visible in logs/reports
   - checkpoints are not advanced incorrectly
   - rerun resumes from the last safe point

### Acceptance Criteria

- no silent data loss
- no checkpoint rollback
- no duplicate explosion after restart beyond understood sink semantics
- operator can explain what happened from logs plus `report`

## Phase 4: Deploy CDSync In Staging AWS

Only deploy after CDC and resume behavior are proven outside the final runtime environment.

### Deployment Shape

- same VPC as the staging PostgreSQL source
- private subnets
- own ECS task definition
- own security group
- no load balancer
- desired count `1`
- on-demand Fargate only

### Why This Shape

- CDSync is a worker, not a web service
- CDC follow mode is effectively an always-on process
- colocating with the source DB minimizes network ambiguity and operational surprises

### Important State Requirement

CDSync currently uses SQLite-backed state and run stats.

Do not treat the ECS deployment as production-grade unless state is on durable storage.

That means one of:

- EFS-backed state/stats files
- a future external durable state backend

Without durable state, task replacement can erase the resume point.

## Phase 5: Run In Parallel With Fivetran

Only after staging CDC, resume behavior, and service deployment are proven.

### Parallel-Run Rules

- Keep Fivetran as the primary path.
- Run CDSync against the same PostgreSQL source in parallel.
- Use a separate BigQuery dataset for CDSync output.
- Give CDSync its own DB user, publication, and replication slot.

### Why Separate Publications And Slots

This prevents operational coupling and makes debugging much easier.

### Comparison Method

Do not compare raw sink tables mechanically.

Instead compare normalized outcomes:

- live row counts
- delete semantics
- freshness/watermarks
- schema shape
- sampled row equality on key business fields

Because Fivetran and CDSync do not model metadata columns identically, compare through normalized SQL views where needed.

## Phase 6: Expand Scope In Staging

After a successful pilot:

1. expand from the first 3 to 5 tables
2. add more varied schemas
3. include larger and higher-churn tables
4. add row-filtered or more complex CDC configurations only after the simpler path is stable

## Phase 7: Production Readiness Gate

Do not start production cutover work until staging has demonstrated:

- clean initial syncs
- stable CDC follow mode
- restart/resume correctness
- acceptable BigQuery cost and latency
- sufficient logging and reporting
- acceptable source load on PostgreSQL

## Open Question: Read Replica CDC

Read-replica CDC is a later optimization, not the first milestone.

### Current Position

- if staging is plain Amazon RDS for PostgreSQL 17, replica-side logical decoding may be viable
- if staging is Aurora PostgreSQL, use the writer
- even if the replica supports logical decoding, we should still prove the writer path first

### Decision Rule

Use the staging read replica for CDSync CDC only after:

1. writer-based CDC works
2. replica-side logical decoding is explicitly validated
3. operational tradeoffs are acceptable

## Immediate Next Steps

1. confirm staging engine type: RDS PostgreSQL vs Aurora PostgreSQL
2. confirm logical replication settings on the staging primary
3. choose the first 3 to 5 pilot tables
4. create the CDSync DB user and publication
5. create the staging shadow BigQuery dataset
6. write `config.staging.yaml`
7. run `validate`, `sync`, `reconcile`, and `report`

## Non-Goals For The First Pass

- replacing Fivetran immediately
- starting with the entire schema
- starting with read-replica CDC
- deploying the service before proving CDC manually
- writing into the same BigQuery dataset as Fivetran
