# Operations

See also: [phased-migration-plan.md](/Users/mazdak/Code/cdsync/docs/phased-migration-plan.md)
See also: [staging-oneoff-ecs.md](/Users/mazdak/Code/cdsync/docs/staging-oneoff-ecs.md)
See also: [how-cdsync-works.md](/Users/mazdak/Code/cdsync/docs/how-cdsync-works.md)
See also: [config-reload-plan.md](/Users/mazdak/Code/cdsync/docs/config-reload-plan.md)

## Runner Mode

Use `run` when you want CDSync to stay alive under a process manager instead of invoking one-shot `sync`.

## Metadata Columns

Destination metadata columns are configurable at the top level:

```yaml
metadata:
  synced_at_column: "_cdsync_synced_at"
  deleted_at_column: "_cdsync_deleted_at"
```

Defaults remain unchanged if omitted.

### PostgreSQL CDC

For PostgreSQL connections with `cdc: true`, `run` uses long-lived CDC follow mode and exits cleanly on `SIGINT` or `SIGTERM`.

```bash
cdsync run --config ./config.yaml --connection app
```

### Polling Sources

For polling-based connections, `run` requires `schedule.every` on the connection and performs incremental syncs on that interval.

Supported `schedule.every` values:

- `30s`
- `5m`
- `1h`
- `1d`
- `15` (defaults to seconds)

## systemd

Install the binary and config, then use the instance unit:

```bash
sudo cp deploy/systemd/cdsync@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cdsync@app
```

This runs:

```bash
cdsync run --config /etc/cdsync/config.yaml --connection app
```

## Container

Build the runner image:

```bash
docker build -f docker/Dockerfile.runner -t cdsync-runner .
```

Run it:

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/etc/cdsync/config.yaml:ro \
  -v $(pwd)/state:/var/lib/cdsync \
  cdsync-runner --config /etc/cdsync/config.yaml --connection app
```

## GitHub Releases

`cdsync` publishes release binaries from GitHub Actions. This repo owns the
binary artifacts only; downstream runtimes are expected to own their own config,
container image assembly, and ECS rollout.

Release artifacts are packaged as:

- `cdsync-x86_64-unknown-linux-gnu.tar.gz`
- `cdsync-aarch64-unknown-linux-gnu.tar.gz`
- matching `.sha256` files

The intended downstream contract is:

1. choose a tagged `cdsync` GitHub release
2. download the Linux artifact for the target runtime
3. build a thin runtime image around that binary
4. inject environment/config from the downstream infra repo
5. deploy the resulting image to the target scheduler

## Real BigQuery

For live BigQuery validation, set:

```bash
export CDSYNC_REAL_BQ_PROJECT="nora-461013"
export CDSYNC_REAL_BQ_DATASET="cdsync_e2e_real"
export CDSYNC_REAL_BQ_LOCATION="US"
export CDSYNC_REAL_BQ_KEY_PATH="/absolute/path/to/service-account.json"
```

Then run the ignored live tests from [e2e.md](/Users/mazdak/Code/cdsync/docs/e2e.md).

## Graceful Shutdown

- `SIGINT` and `SIGTERM` are handled.
- Polling runners finish the current sync cycle, then exit.
- CDC runners stop following the stream after the current transaction boundary and persist the last known checkpoint.
