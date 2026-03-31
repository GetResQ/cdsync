# Mission

Mission: Replace NDJSON as the primary BigQuery GCS batch-load format with Parquet, while keeping a narrowly-scoped fallback only for schema types that are not yet safe to encode through the current Parquet path.

## Done Criteria

1. BigQuery batch-load uploads use Parquet by default.
2. Load jobs are configured for Parquet objects.
3. The current frame-to-Parquet path preserves the supported BigQuery column types we already emit in bulk loads.
4. Unsupported schema shapes fall back intentionally instead of silently writing invalid Parquet.
5. The change is covered by focused tests and passes formatting, tests, and clippy.

## Guardrails

- Keep the change bounded to the batch-load path; do not redesign the whole destination writer.
- Do not regress emulator behavior or the non-batch-load append/upsert paths.
- Do not claim Parquet support for schema types we cannot encode safely yet.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: Use Parquet as the primary batch-load format immediately rather than adding a long-term config switch between NDJSON and Parquet.
- Decision: Keep NDJSON only as a temporary compatibility path for schema types like `NUMERIC` and `JSON` that are not safely represented by the current DataFrame-to-Parquet conversion.
- Constraint: The existing bulk frames store several logical types as strings, so supported Parquet loads need an explicit frame-to-typed-Parquet conversion step instead of writing the raw frame bytes directly.
