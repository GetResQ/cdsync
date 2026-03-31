# Mission

Mission: Start enforcing a file-size and module-separation rule by breaking apart oversized Rust files along responsibility boundaries instead of continuing to accumulate large grab-bag modules.

## Done Criteria

1. Files over 1k LOC are reduced where the split is low-risk and logically clean.
2. The extracted modules represent real responsibility boundaries, not arbitrary shuffling.
3. The refactor preserves behavior and keeps tests/clippy clean.

## Guardrails

- Prefer structural refactors over semantic rewrites.
- Keep public APIs stable unless a break is necessary.
- Do not mix unrelated feature work into the reorganization pass.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: `bigquery.rs` is easiest to split first because it already has clear batch-load, storage-write, and value-conversion seams.
- Decision: `main.rs` can come back under the cap by extracting status/report/reconcile operations and test modules without touching the sync/run path yet.
- Constraint: `src/sources/postgres.rs` is still the largest remaining hard violation and will need a larger CDC/snapshot extraction than the lighter refactors completed here.
