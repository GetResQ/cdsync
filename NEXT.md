# Next

## After Staging Stabilization

### Realtime Operator Stream

Add a realtime admin stream after the live staging situation is understood and stable.

Recommendation:
- Start with `SSE`, not WebSocket.
- Keep it server-to-client only for the first slice.
- Preserve the existing HTTP admin endpoints as the source of truth.

Primary goal:
- Answer "is this connection healthy, making progress, idle, or stuck right now?"

Proposed stream shape:
- `GET /v1/stream?connection=<id>`
- Send a snapshot on connect, then small delta events.
- Include `seq` and `at` on every event.

First event types:
- `service.heartbeat`
  version, deploy revision, uptime, server time
- `connection.runtime`
  phase, reason, mode, run id, last error summary, last successful apply time
- `connection.throughput`
  rows read/written/upserted/deleted totals and rolling rates, extract/load ms
- `connection.cdc`
  slot active, last received/flushed LSN, pending events, queued commits, inflight applies, backpressure state, last xlog activity time
- `snapshot.progress`
  current table, chunks complete/total, rows copied, rows/sec, estimated remaining if cheap to compute
- `table.progress`
  only for active/hot tables, not the full table set every tick

Do not include:
- per-row CDC payloads
- raw WAL / decoded tuples
- full config blobs
- full checkpoint maps every second
- giant full-table snapshots on every tick
- secrets, auth material, SQL text

Implementation notes:
- Stream from in-memory runtime state and live stats handles, not by polling only the stats DB.
- Keep DB persistence for durability/history.
- Use low-frequency heartbeats plus change-driven events where possible.
- Make the stream connection-scoped to avoid unnecessary fanout.

### WAL / Slot Visibility

Add explicit source-side WAL / replication-slot visibility to the admin surface and dashboard so operators can distinguish:
- no new source WAL
- source WAL is advancing and CDSync is following
- source WAL is advancing but CDSync is not advancing
- slot is retaining WAL and creating storage risk

Do not show only a generic "WAL size" number. The useful signals are:
- `current_wal_lsn`
- `confirmed_flush_lsn`
- `restart_lsn`
- `slot_active`
- `wal_bytes_behind_confirmed = pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`
- `wal_bytes_retained_by_slot = pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`
- `last_xlog_activity_at` or equivalent recent source activity signal

Desired dashboard/operator questions:
- Is Postgres producing new WAL right now?
- Is CDSync acknowledging and flushing WAL?
- Is slot retention growing?
- Is the source idle, or is CDSync stale?

Good first UI fields:
- source WAL advancing: yes/no
- CDSync flush advancing: yes/no
- bytes behind confirmed flush
- bytes retained by slot
- slot active: yes/no
- last WAL activity age
