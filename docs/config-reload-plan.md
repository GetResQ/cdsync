# Config Reload Plan

This document describes:

1. how CDSync behaves **today**
2. how we should **deploy config changes**
3. how the process should **notice and apply changes**
4. which changes are safe to resume versus which changes require resync or operator action

## Current Behavior

Today, CDSync does **not** hot-reload configuration.

### `sync`

- loads the config once
- runs one sync
- exits

### `run`

- loads the config once at startup
- picks one connection by ID
- then keeps looping or following CDC with that in-memory config
- does **not** watch the config file for changes

This means:

- if you edit `config.toml` while the process is running, the running process does not notice
- the process only sees config changes after a restart or replacement task

That is the real behavior in the current code.

## Recommended Deployment Model

For production, config reload should be handled as a **deployment event**, not as ad-hoc file watching inside the process.

That means:

1. build a new config artifact
2. deploy a new ECS task definition / service revision
3. start a new task with the new config
4. let CDSync resume from durable state

This is safer than live file watching for CDC because it keeps rollout behavior explicit and observable.

## Why Restart-Based Reload Is Better First

Hot reload sounds convenient, but it creates difficult transition questions:

- what happens if the publication changes mid-stream?
- what happens if table selection changes while CDC is running?
- what happens if a filter changes but the process is halfway through a transaction batch?

For CDC systems, those are correctness questions, not just convenience questions.

So the first production-grade answer should be:

- **config changes are applied by restart**
- **state survives restart**
- **the new process computes what action to take**

## Deployment Plan

### Phase 1: immutable config per deployment

Use one of these patterns:

- bundle config into the image
- render config at deploy time into the container filesystem
- inject config through SSM/Secrets + entrypoint rendering

The important part is:

- the task starts with one fully-defined config version
- that config does not change under its feet

### Phase 2: version the config explicitly

Every startup should log:

- connection ID
- config path
- config hash or version

That gives operators an answer to:

- "which config is this task actually using?"

### Phase 3: deploy by task replacement

For ECS:

- register a new task definition revision
- replace the running task
- let the new task resume from saved state

For one-off tasks:

- start a fresh task with the new config

For long-running CDC services:

- desired count `1`
- rolling replacement or stop/start replacement
- rely on CDSync lease/state handling to avoid overlapping ownership

## How The Process Should Notice Changes

### Today

It does not notice them at runtime.

### Recommended near-term behavior

At process startup:

1. load config
2. compute config hash
3. compare to last applied config hash in state
4. classify the change
5. decide whether to:
   - resume normally
   - mark tables for resync
   - fail fast and require operator action

That gives us deterministic behavior without in-process watchers.

## How The Process Should Take Action

Not all config changes are equal. CDSync should treat them differently.

## Change Matrix

### Safe resume changes

Examples:

- log level
- retry/backoff values
- concurrency values
- telemetry endpoints

Action:

- restart task
- resume from saved state
- no resync needed

### Table addition

#### Polling connections

Action:

- restart
- new table is discovered
- next run backfills it

#### CDC connections

Action:

- update PostgreSQL publication
- restart
- mark the new table for snapshot/backfill

Important:

- this is not just "resume from WAL"
- the process must know the table is new and perform a snapshot for it

### Table removal

Action:

- restart
- stop syncing that table
- do not delete destination data automatically

### Column include/exclude change

Action:

- restart
- treat as table resync for affected tables

Reason:

- the destination shape has changed
- historical rows likely need rewriting

### Row filter change

Action:

- restart
- treat as table resync for affected tables

Reason:

- existing destination rows may no longer match the source selection

### Publication change

Action:

- restart
- revalidate publication
- if table membership changed, mark affected tables for snapshot/resync

### Metadata column name change

Examples:

- `_cdsync_synced_at`
- `_cdsync_deleted_at`

Action:

- treat as breaking destination schema change
- do not live-reload
- require explicit migration or fresh destination plan

### Destination dataset/table naming change

Action:

- treat as cutover
- do not resume blindly
- require explicit operator intent

## Proposed CDSync Startup Logic

On startup, CDSync should eventually do this:

1. load config
2. compute config hash
3. load previous applied config hash and per-table applied config state
4. diff old vs new config
5. classify the diff
6. build an action plan:
   - `resume`
   - `resnapshot tables A,B,C`
   - `fail until operator updates publication`
   - `fail because change is destructive`
7. log the plan clearly
8. execute it

## Hot Reload: Later, Not First

If we ever add live config reload, it should be a second phase.

That version should still:

- compute config diff
- classify change safety
- reject unsafe live changes
- only apply a small safe subset in-place

Examples of changes that might be safe live later:

- log level
- telemetry endpoint
- retry/backoff numbers

Examples that should still force restart:

- table set
- publication
- row filters
- destination schema semantics
- source credentials

## Operational Recommendation

Until the config-diff and targeted resync machinery is fully implemented, the safe operator model is:

1. treat config as immutable for each task
2. deploy config changes by starting a new task revision
3. assume:
   - new CDC tables need publication changes plus backfill
   - changed filters/column selection need resync
4. do not rely on live file edits being noticed
