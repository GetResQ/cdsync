# End-to-end tests (docker)

These tests are ignored by default and require a Postgres instance plus a BigQuery emulator.

## Start services

Start only the BigQuery emulator:

```
docker compose -f docker-compose.e2e.yml up -d bigquery-emulator
```

Optionally start the Postgres container (disabled by default to avoid clashing with other local Postgres):

```
docker compose -f docker-compose.e2e.yml --profile itest up -d postgres
```

## Environment variables

Set the connection info before running the tests:

```
export CDSYNC_E2E_PG_URL="postgres://cdsync:cdsync@localhost:5433/cdsync"
export CDSYNC_E2E_BQ_HTTP="http://localhost:9050"
export CDSYNC_E2E_BQ_GRPC="localhost:9051"
export CDSYNC_E2E_BQ_PROJECT="cdsync"
export CDSYNC_E2E_BQ_DATASET="cdsync_e2e"
```

If you already run Postgres elsewhere, point `CDSYNC_E2E_PG_URL` at that instance and skip the dockerized Postgres service.

## Run the test

```
cargo test --test e2e_postgres_bigquery -- --ignored --nocapture
```
