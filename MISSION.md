# Mission

Mission: Close the next live-progress persistence test gaps by proving that periodic run-stat flushing and snapshot checkpoint writes actually persist observable state during an in-flight run.

## Done Criteria

1. There is coverage for periodic run-stat persistence to the stats database.
2. There is coverage for snapshot checkpoint writes during snapshot progress, not just at final completion.
3. The tests run in the normal suite and keep formatting, tests, and clippy clean.

## Guardrails

- Keep the work bounded to tests and the minimum helper seams needed to make those tests reliable.
- Do not weaken the existing persistence behavior just to make tests easier.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: Admin API route coverage now exists through in-process server tests, so the next highest-value test work is live-progress persistence.
- Constraint: Persistence tests are easiest to write where the runtime already exposes stable helper seams, rather than trying to drive a full external integration path for every case.
