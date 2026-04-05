# CDC Internal Role Architecture

This document defines the next CDSync CDC architecture for PostgreSQL follow mode
when BigQuery batch load is the sink path.

The design stays inside the current app:

- one binary
- one repo
- one `cdsync run` entrypoint
- shared state DB
- shared admin API

It deliberately stops short of introducing Kafka/SQS or splitting into separate
deployable services. The key change is internal role separation and durable
handoff boundaries.

## Goals

1. Keep the WAL reader out of BigQuery control-plane latency.
2. Keep WAL acknowledgement correct.
3. Scale the heavy sink work independently inside the same process.
4. Make it obvious from logs, metrics, spans, and the dashboard whether:
   - producer is blocked
   - queue is backing up
   - consumers are slow
   - coordinator is head-of-line blocked
   - one table family dominates throughput
5. Make restart/resume semantics explicit and testable.

## Non-Goals

- Acking WAL after GCS upload alone.
- Introducing an external broker in the first implementation.
- Hiding queue/coordinator state behind vague "running" dashboard labels.

## Current Runtime Shape

Today, the CDC follow loop still owns too much:

```text
Postgres replication stream
  -> decode messages
  -> build pending tx events
  -> split by table
  -> dispatch table apply
  -> upload parquet / enqueue queued batch job
  -> wait for completion
  -> advance watermark
  -> send replication status update
  -> persist last_lsn
```

Even with the queued batch-load path, too much coordination still happens in the
main follow loop. The process is "alive" when:

- producer can read WAL
- consumer can run merges
- coordinator can advance the watermark

but the code still makes those concerns too easy to blur together.

## Target Runtime Shape

`cdsync run` starts four internal services per managed CDC connection:

```text
                                   +----------------------+
                                   |   Admin / Sampler    |
                                   |  API + diagnostics   |
                                   +----------+-----------+
                                              |
                                              v
+--------------------+     durable DB state   |     metrics / traces / logs
|    Producer        |------------------------+--------------------------+
| read WAL           |                        |                          |
| decode tx          |                        |                          |
| batch by table     |                        |                          |
| build parquet      |                        |                          |
| upload to GCS      |                        |                          |
| persist jobs       |                        |                          |
| persist fragments   \                       |                          |
| publish work ------->\----------------------+                          |
+----------+-----------+ \                                                 
           |              \                                                
           |               \                                              
           |                v                                             
           |        +-------+--------+                                    
           |        |   Consumer     |                                    
           |        | worker pool    |                                    
           |        | claim jobs     |                                    
           |        | ensure target  |                                    
           |        | ensure staging |                                    
           |        | load job       |                                    
           |        | merge          |                                    
           |        | mark job done  |                                    
           |        +-------+--------+                                    
           |                |                                             
           |                v                                             
           |        +-------+--------+                                    
           +------->| Coordinator    |                                    
                    | complete seqs  |                                    
                    | head-of-line   |                                    
                    | advance WAL    |                                    
                    | persist lastlsn|                                    
                    +----------------+                                    
```

This is still one process. The separation is logical:

- producer control loop
- consumer worker pool
- coordinator control loop
- admin/control plane

## `cdsync run` Process Model

The default process shape is:

```text
cdsync run
|
|-- connection supervisor
|   `-- one runtime bundle per selected connection
|       |
|       |-- producer task
|       |-- consumer scheduler task
|       |-- consumer worker pool
|       |-- coordinator task
|       `-- checkpoint-age reporter
|
`-- admin API runtime/thread
```

The default `run` path launches all roles. Later, we may expose a role filter,
but it is not required for the initial implementation.

## Role Responsibilities

### Producer

Owns:

- replication connection
- transaction buffering
- table batching
- parquet/frame build
- GCS artifact upload
- durable job + fragment persistence
- backpressure decisions

Does **not** own:

- BigQuery load-job polling
- merge completion
- WAL acknowledgement

Producer output boundary:

- durable job row exists
- durable fragment rows exist
- artifact URI exists

At that point the producer may move on, but it may not acknowledge WAL yet.

### Consumer

Owns:

- claiming runnable jobs from durable state
- enforcing per-table work serialization
- ensure target table
- ensure staging table
- load job
- merge
- best-effort cleanup
- marking job + fragment completion/failure durably

Consumer does not:

- own the replication socket
- advance WAL directly

### Coordinator

Owns:

- head-of-line ordered commit tracking
- fragment completion accounting
- deciding which commit LSN is safe to acknowledge
- sending replication status update
- persisting `postgres_cdc_last_lsn`
- recovery on restart

Coordinator does not:

- build artifacts
- talk to BigQuery except through state-driven decisions

### Admin / Sampler

Owns:

- current operational view
- CDC slot stats
- queue stats
- throughput stats
- top-table diagnostics

It must remain observational and must not block producer/consumer/coordinator
loops.

## Durable State Model

The current `cdc_batch_load_jobs` table is a useful first step, but the full
design needs explicit fragment/coordinator state.

### Existing table

`cdc_batch_load_jobs`

Keeps:

- job id
- table key
- first sequence
- job payload
- job status
- attempts / errors
- timestamps

### New tables required

#### `cdc_commit_fragments`

One row per durable fragment that must complete before a commit sequence can be
acknowledged.

Suggested columns:

- `connection_id`
- `fragment_id`
- `sequence`
- `commit_lsn`
- `table_key`
- `job_id`
- `fragment_status`
- `row_count`
- `upserted_count`
- `deleted_count`
- `created_at`
- `updated_at`
- `last_error`

Important rule:

- multiple fragments may point to the same `job_id`
- watermark advances only when every fragment for the oldest unacked sequence is
  complete

#### `cdc_watermark_state`

One row per connection.

Suggested columns:

- `connection_id`
- `next_sequence_to_ack`
- `last_enqueued_sequence`
- `last_received_lsn`
- `last_flushed_lsn`
- `last_persisted_lsn`
- `updated_at`

This table makes restart/recovery explicit instead of reconstructing everything
from in-memory trackers.

#### Optional later: `cdc_job_attempts`

For deep debugging and SLO reporting:

- one row per consumer attempt
- stage durations
- failure reason

This is useful, but not required for the first cut if spans/logs are already
good.

## End-to-End Flow

### 1. Producer path

```text
WAL tx commit
  -> split by table
  -> compact per table
  -> build frames
  -> upload artifact(s) to GCS
  -> insert cdc_batch_load_jobs
  -> insert cdc_commit_fragments
  -> update cdc_watermark_state.last_enqueued_sequence
  -> emit producer metrics/logs/spans
```

### 2. Consumer path

```text
poll/claim runnable job
  -> ensure target
  -> ensure staging
  -> load job
  -> merge
  -> cleanup
  -> mark job succeeded
  -> mark linked fragments succeeded
  -> emit consumer metrics/logs/spans
```

If the consumer fails:

- job is marked failed or retriable
- fragment remains incomplete
- coordinator does not advance WAL

### 3. Coordinator path

```text
observe completed fragments
  -> check oldest unacked sequence
  -> if all fragments complete for contiguous sequences:
       - advance last_flushed_lsn
       - send status update to replication stream owner
       - persist postgres_cdc_last_lsn
       - update cdc_watermark_state
  -> emit coordinator metrics/logs/spans
```

## Backpressure Rules

Backpressure is required, but it must be applied at the durable boundary, not by
making the producer await BigQuery work.

Producer should slow down when:

- pending/running durable jobs exceed a connection threshold
- unacked fragment count exceeds a threshold
- queue age exceeds a threshold
- GCS upload failures exceed a threshold

Suggested first thresholds:

- `max_pending_jobs`
- `max_unacked_sequences`
- `max_oldest_pending_age`

Backpressure actions:

- stop reading new replication messages temporarily
- keep connection alive with status heartbeats
- emit `producer_backpressure` logs and metrics

## Per-Table Ordering

Ordering rules:

- same-table jobs must execute serially
- different tables may execute concurrently
- WAL acknowledgement is still commit-sequence ordered

That means:

- a single table cannot be processed by two consumers at once
- multiple different tables can be processed by multiple consumers
- head-of-line commit ordering is enforced by the coordinator, not by globally
  serializing all consumers

## Coalescing

Coalescing belongs in the consumer-side scheduler, not the producer.

The safe first coalescing rule:

- combine adjacent pending jobs for the same table
- preserve order
- keep the earliest `first_sequence`
- preserve fragment membership

This reduces:

- staging table churn
- load-job overhead
- merge overhead

without making the producer more complex.

## Instrumentation Requirements

Every stage must have:

- structured logs
- OTLP spans
- OTLP metrics

### Producer logs/spans/metrics

Spans:

- `cdc_producer.receive_message`
- `cdc_producer.decode`
- `cdc_producer.compact_table_batch`
- `cdc_producer.build_frame`
- `cdc_producer.upload_artifact`
- `cdc_producer.persist_job`
- `cdc_producer.persist_fragment`

Metrics:

- `cdsync_cdc_producer_messages_total`
- `cdsync_cdc_producer_rows_total`
- `cdsync_cdc_producer_artifact_upload_ms`
- `cdsync_cdc_unacked_sequences`
- `cdsync_cdc_enqueued_jobs_total`

Logs:

- artifact uploaded
- job persisted
- fragment persisted
- producer backpressure entered/exited

### Consumer logs/spans/metrics

Spans:

- `cdc_consumer.claim_job`
- `cdc_consumer.ensure_target`
- `cdc_consumer.ensure_staging`
- `cdc_consumer.load_job`
- `cdc_consumer.merge`
- `cdc_consumer.cleanup`

Metrics:

- `cdsync_cdc_consumer_jobs_started_total`
- `cdsync_cdc_consumer_jobs_succeeded_total`
- `cdsync_cdc_consumer_jobs_failed_total`
- `cdsync_cdc_consumer_job_duration_ms`
- `cdsync_cdc_consumer_stage_duration_ms{stage=...}`
- `cdsync_cdc_consumer_loaded_rows_total`

Logs:

- job claimed
- job succeeded / failed
- each stage duration
- retry scheduling

### Coordinator logs/spans/metrics

Spans:

- `cdc_coordinator.observe_completion`
- `cdc_coordinator.advance_watermark`
- `cdc_coordinator.send_status_update`
- `cdc_coordinator.persist_last_lsn`

Metrics:

- `cdsync_cdc_coordinator_advances_total`
- `cdsync_cdc_coordinator_advance_ms`
- `cdsync_cdc_confirmed_flush_lsn_gap_bytes`
- `cdsync_cdc_restart_lsn_gap_bytes`
- `cdsync_cdc_head_sequence_age_seconds`

Logs:

- head sequence blocked
- watermark advanced
- status update sent
- last_lsn persisted

## Admin API / Dashboard Requirements

Primary fields:

- queued jobs
- running jobs
- failed jobs
- succeeded jobs
- oldest pending
- oldest running
- jobs/min
- rows/min
- avg load+merge sec/job
- top queued tables
- top load time tables

Remove from the main card:

- `First Inflight Seq`
- `First Inflight Table`

Those are still useful for debugging, but they are secondary diagnostics, not
primary health signals.

Secondary diagnostics:

- oldest unacked sequence
- oldest unacked sequence age
- producer backpressure state
- consumer worker utilization
- latest failed job error

## Failure Recovery Rules

On restart:

1. producer reloads watermark state
2. consumer reclaims `pending` and stale `running` jobs
3. coordinator rebuilds fragment state from durable tables
4. coordinator resumes acknowledgement only after confirming contiguous
   completed sequences

Important:

- `running` jobs from a dead process must be reclaimable
- claim logic must be lease-based or timeout-based

## Testing Matrix

### Unit tests

- fragment completion -> contiguous watermark advancement
- non-contiguous completion does not advance
- same-table serialization
- coalescing preserves order and counts
- producer backpressure thresholds
- stale running job reclamation

### Integration tests

- state-store tests for job/fragment/watermark transitions
- consumer claim tests with `FOR UPDATE SKIP LOCKED`
- coordinator recovery from interrupted process

### E2E tests

- follow mode with real Postgres logical replication + BigQuery emulator
- follow mode with real GCS upload and real BigQuery batch load when creds exist
- schema change with new column
- soft delete flow
- task replacement during queued CDC load
- duplicate process startup with connection lock
- no-WAL-ack on consumer failure

### Destructive tests

- kill process after job persisted but before fragment persisted
- kill process after fragment persisted but before consumer claim
- kill process after merge succeeds but before coordinator persists last_lsn
- simulate long-running head-of-line block

## Implementation Order

1. Finish the architecture/spec and wire all observability requirements.
2. Add the missing durable coordinator tables.
3. Move watermark coordination out of the producer loop into an explicit
   coordinator service.
4. Move job execution into an explicit consumer scheduler/worker pool that
   claims durable work.
5. Add coalescing and staging-table reuse only after the role split is correct.

## Acceptance Criteria

The redesign is not complete until we can answer, from runtime data:

- Are we dominated by one table or many?
- Are consumers starved, saturated, or blocked?
- Is the coordinator head-of-line blocked?
- Is the producer applying backpressure?
- How long do we spend in ensure target, ensure staging, load job, and merge?
- Are we actually draining WAL lag over a multi-minute window?
