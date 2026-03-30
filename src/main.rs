mod config;
mod destinations;
mod dotenv;
mod runner;
mod sources;
mod state;
mod stats;
mod telemetry;
mod types;

use crate::config::{Config, DestinationConfig, SourceConfig};
use crate::destinations::bigquery::BigQueryDestination;
use crate::runner::{ShutdownController, ShutdownSignal, schedule_interval};
use crate::sources::postgres::{CdcSyncRequest, PostgresSource, TableSyncRequest};
use crate::sources::salesforce::{SalesforceSource, SalesforceSyncRequest};
use crate::state::{ConnectionState, SyncState, SyncStateStore};
use crate::stats::{StatsDb, StatsHandle};
use crate::types::{SyncMode, TableCheckpoint};
use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "cdsync", version, about = "CDSync - Open-source data sync")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(long)]
        config: PathBuf,
    },
    Sync {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        full: bool,
        #[arg(long)]
        incremental: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        schema_diff: bool,
        #[arg(long)]
        follow: bool,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: String,
    },
    Status {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
    },
    Validate {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        verbose: bool,
    },
    Report {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    Reconcile {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: String,
        #[arg(long)]
        table: Option<String>,
    },
}

struct SyncConnectionRequest<'a> {
    connection: &'a crate::config::ConnectionConfig,
    state: &'a mut ConnectionState,
    state_handle: crate::state::StateHandle,
    metadata: crate::types::MetadataColumns,
    mode: SyncMode,
    dry_run: bool,
    follow: bool,
    default_batch_size: usize,
    max_retries: u32,
    max_concurrency: usize,
    retry_backoff_ms: u64,
    schema_diff_enabled: bool,
    run_id: Option<String>,
    stats: Option<StatsHandle>,
    shutdown: Option<ShutdownSignal>,
}

struct SyncCommandRequest {
    config_path: PathBuf,
    full: bool,
    incremental: bool,
    dry_run: bool,
    connection_filter: Option<String>,
    schema_diff_enabled: bool,
    follow: bool,
    shutdown: Option<ShutdownSignal>,
}

#[derive(Serialize)]
struct ReportOutput {
    state: serde_json::Value,
    recent_runs: Vec<crate::stats::RunSummary>,
}

#[derive(Serialize)]
struct ReconciliationOutput {
    connection_id: String,
    tables: Vec<TableReconciliationReport>,
}

#[derive(Serialize)]
struct TableReconciliationReport {
    source_table: String,
    destination_table: String,
    source_row_count: Option<i64>,
    destination_row_count: Option<i64>,
    deleted_rows: Option<i64>,
    source_max_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    destination_max_synced_at: Option<chrono::DateTime<chrono::Utc>>,
    count_match: Option<bool>,
    error: Option<String>,
}

fn select_sync_connections<'a>(
    connections: &'a [crate::config::ConnectionConfig],
    filter: Option<&str>,
) -> Result<Vec<&'a crate::config::ConnectionConfig>> {
    if let Some(filter) = filter {
        let connection = connections
            .iter()
            .find(|connection| connection.id == filter)
            .context("connection not found")?;
        if !connection.enabled() {
            anyhow::bail!("connection {} is disabled", connection.id);
        }
        return Ok(vec![connection]);
    }

    Ok(connections
        .iter()
        .filter(|connection| connection.enabled())
        .collect())
}

fn reconcile_count_match(
    source_summary: &crate::sources::postgres::PostgresTableSummary,
    destination_summary: &crate::destinations::bigquery::DestinationTableSummary,
    table: &crate::sources::postgres::ResolvedPostgresTable,
) -> bool {
    let live_destination_rows = if table.soft_delete {
        destination_summary
            .row_count
            .saturating_sub(destination_summary.deleted_rows)
    } else {
        destination_summary.row_count
    };
    source_summary.row_count == live_destination_rows
}

async fn load_latest_postgres_checkpoint(
    state_handle: &crate::state::StateHandle,
    table_name: &str,
    fallback: &TableCheckpoint,
) -> TableCheckpoint {
    match state_handle.load_postgres_checkpoint(table_name).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => fallback.clone(),
        Err(err) => {
            warn!(
                table = %table_name,
                error = %err,
                "failed to load persisted postgres checkpoint after interrupted retry; using in-memory checkpoint"
            );
            fallback.clone()
        }
    }
}

async fn load_latest_salesforce_checkpoint(
    state_handle: &crate::state::StateHandle,
    object_name: &str,
    fallback: &TableCheckpoint,
) -> TableCheckpoint {
    match state_handle.load_salesforce_checkpoint(object_name).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => fallback.clone(),
        Err(err) => {
            warn!(
                object = %object_name,
                error = %err,
                "failed to load persisted salesforce checkpoint after interrupted retry; using in-memory checkpoint"
            );
            fallback.clone()
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::load_dotenv()?;
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { config } => cmd_init(config).await,
        Commands::Sync {
            config,
            full,
            incremental,
            dry_run,
            connection,
            schema_diff,
            follow,
        } => {
            cmd_sync(SyncCommandRequest {
                config_path: config,
                full,
                incremental,
                dry_run,
                connection_filter: connection,
                schema_diff_enabled: schema_diff,
                follow,
                shutdown: None,
            })
            .await
        }
        Commands::Run { config, connection } => cmd_run(config, connection).await,
        Commands::Status { config, connection } => cmd_status(config, connection).await,
        Commands::Validate {
            config,
            connection,
            verbose,
        } => cmd_validate(config, connection, verbose).await,
        Commands::Report {
            config,
            connection,
            limit,
        } => cmd_report(config, connection, limit).await,
        Commands::Reconcile {
            config,
            connection,
            table,
        } => cmd_reconcile(config, connection, table).await,
    }
}

async fn cmd_init(config_path: PathBuf) -> Result<()> {
    if !config_path.exists() {
        let template = Config::template(&config_path);
        tokio::fs::write(&template.path, template.content).await?;
        println!("Created config template at {}", template.path.display());
        return Ok(());
    }

    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    for connection in &cfg.connections {
        if !connection.enabled() {
            continue;
        }
        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
                let tables = source.resolve_tables().await?;
                for table in &tables {
                    let schema = source.discover_schema(table).await?;
                    info!(
                        "postgres schema: {} ({} columns)",
                        schema.name,
                        schema.columns.len()
                    );
                }
            }
            SourceConfig::Salesforce(sf) => {
                let source = SalesforceSource::new(sf.clone(), metadata.clone())?;
                source.validate().await?;
            }
        }

        match &connection.destination {
            DestinationConfig::BigQuery(bq) => {
                let dest = BigQueryDestination::new(bq.clone(), false, metadata.clone()).await?;
                dest.validate().await?;
            }
        }
    }

    println!("Config validation complete.");
    Ok(())
}

async fn cmd_sync(request: SyncCommandRequest) -> Result<()> {
    let SyncCommandRequest {
        config_path,
        full,
        incremental,
        dry_run,
        connection_filter,
        schema_diff_enabled,
        follow,
        shutdown,
    } = request;
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    if follow && connection_filter.is_none() {
        anyhow::bail!("--follow requires --connection for a single postgres CDC connection");
    }

    let state_store = SyncStateStore::open(&cfg.state.path).await?;
    let mode = match (full, incremental) {
        (true, false) => SyncMode::Full,
        (false, true) => SyncMode::Incremental,
        (false, false) => SyncMode::Incremental,
        (true, true) => {
            anyhow::bail!("--full and --incremental are mutually exclusive");
        }
    };

    let mut state = state_store.load_state().await?;
    let stats_db = if let Some(stats_cfg) = &cfg.stats {
        Some(StatsDb::new(&stats_cfg.path).await?)
    } else {
        None
    };
    let default_batch_size = cfg
        .sync
        .as_ref()
        .and_then(|s| s.default_batch_size)
        .unwrap_or(10_000);
    let max_retries = cfg.sync.as_ref().and_then(|s| s.max_retries).unwrap_or(3);
    let max_concurrency = cfg
        .sync
        .as_ref()
        .and_then(|s| s.max_concurrency)
        .unwrap_or(1)
        .max(1);
    let retry_backoff_ms = cfg
        .sync
        .as_ref()
        .and_then(|s| s.retry_backoff_ms)
        .unwrap_or(1000);
    let selected_connections =
        select_sync_connections(&cfg.connections, connection_filter.as_deref())?;

    for connection in selected_connections {
        let connection_state = state
            .connections
            .entry(connection.id.clone())
            .or_insert_with(ConnectionState::default);
        let lease = state_store.acquire_connection_lock(&connection.id).await?;
        let state_handle = state_store.handle(&connection.id);
        connection_state.last_sync_started_at = Some(Utc::now());
        connection_state.last_sync_status = Some("running".to_string());
        connection_state.last_error = None;
        state_handle.save_connection_meta(connection_state).await?;

        let stats_handle = stats_db.as_ref().map(|_| StatsHandle::new(&connection.id));
        let run_id = match &stats_handle {
            Some(handle) => Some(handle.run_id().await),
            None => None,
        };
        info!(
            connection = %connection.id,
            run_id = run_id.as_deref().unwrap_or("none"),
            mode = ?mode,
            dry_run,
            follow,
            "starting connection sync"
        );

        let result = sync_connection(SyncConnectionRequest {
            connection,
            state: connection_state,
            state_handle: state_handle.clone(),
            metadata: metadata.clone(),
            mode,
            dry_run,
            follow,
            default_batch_size,
            max_retries,
            max_concurrency,
            retry_backoff_ms,
            schema_diff_enabled,
            run_id: run_id.clone(),
            stats: stats_handle.clone(),
            shutdown: shutdown.clone(),
        })
        .await;

        connection_state.last_sync_finished_at = Some(Utc::now());
        let error_string = result.as_ref().err().map(|err| err.to_string());
        match &result {
            Ok(_) => {
                connection_state.last_sync_status = Some("success".to_string());
                info!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    "connection sync completed successfully"
                );
            }
            Err(err) => {
                connection_state.last_sync_status = Some("failed".to_string());
                connection_state.last_error = error_string.clone();
                error!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    error = %err,
                    "connection sync failed"
                );
            }
        }
        if let Some(handle) = &stats_handle {
            let status = if result.is_ok() { "success" } else { "failed" };
            handle.finish(status, error_string).await;
            if let Some(db) = &stats_db {
                db.persist_run(handle).await?;
            }
        }
        let duration_ms = connection_state
            .last_sync_started_at
            .zip(connection_state.last_sync_finished_at)
            .map(|(started_at, finished_at)| (finished_at - started_at).num_milliseconds() as f64)
            .unwrap_or(0.0);
        telemetry::record_sync_run(
            &connection.id,
            connection_state
                .last_sync_status
                .as_deref()
                .unwrap_or("unknown"),
            duration_ms,
        );
        state_handle.save_connection_state(connection_state).await?;
        lease.release().await?;

        result?;
    }

    println!("Sync complete.");
    Ok(())
}

async fn sync_connection(request: SyncConnectionRequest<'_>) -> Result<()> {
    let SyncConnectionRequest {
        connection,
        state,
        state_handle,
        metadata,
        mode,
        dry_run,
        follow,
        default_batch_size,
        max_retries,
        max_concurrency,
        retry_backoff_ms,
        schema_diff_enabled,
        run_id,
        stats,
        shutdown,
    } = request;
    let dest = match &connection.destination {
        DestinationConfig::BigQuery(bq) => {
            BigQueryDestination::new(bq.clone(), dry_run, metadata.clone()).await?
        }
    };
    dest.validate().await?;

    match &connection.source {
        SourceConfig::Postgres(pg) => {
            let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
            let tables = source.resolve_tables().await?;
            if follow && !source.cdc_enabled() {
                anyhow::bail!("--follow requires postgres.cdc=true");
            }
            if source.cdc_enabled() {
                info!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    "syncing postgres via CDC"
                );
                let mut attempt = 0;
                let mut backoff = Duration::from_millis(retry_backoff_ms);
                loop {
                    attempt += 1;
                    let result = source
                        .sync_cdc(CdcSyncRequest {
                            dest: &dest,
                            state,
                            state_handle: Some(state_handle.clone()),
                            mode,
                            dry_run,
                            follow,
                            default_batch_size,
                            tables: &tables,
                            schema_diff_enabled,
                            stats: stats.clone(),
                            shutdown: shutdown.clone(),
                        })
                        .await
                        .with_context(|| "syncing postgres CDC");
                    match result {
                        Ok(_) => break,
                        Err(err) if follow || attempt < max_retries => {
                            telemetry::record_retry_attempt(&connection.id, "postgres_cdc");
                            warn!(
                                connection = %connection.id,
                                run_id = run_id.as_deref().unwrap_or("none"),
                                attempt,
                                backoff_ms = backoff.as_millis() as u64,
                                error = %err,
                                "postgres CDC sync attempt failed; retrying"
                            );
                            if wait_backoff(backoff, shutdown.clone()).await {
                                info!(
                                    connection = %connection.id,
                                    run_id = run_id.as_deref().unwrap_or("none"),
                                    attempt,
                                    "shutdown requested during postgres CDC retry backoff"
                                );
                                return Ok(());
                            }
                            backoff = Duration::from_millis(backoff.as_millis() as u64 * 2);
                            continue;
                        }
                        Err(err) => {
                            error!(
                                connection = %connection.id,
                                run_id = run_id.as_deref().unwrap_or("none"),
                                attempt,
                                error = %err,
                                "postgres CDC sync attempt failed permanently"
                            );
                            return Err(err);
                        }
                    }
                }
            } else {
                if max_concurrency > source.pool_max_connections() as usize {
                    warn!(
                        max_concurrency,
                        pool_max = source.pool_max_connections(),
                        "max_concurrency exceeds postgres pool size; reduce sync.max_concurrency or increase pool size"
                    );
                }
                let source = Arc::new(source);
                let dest = Arc::new(dest);
                let semaphore = Arc::new(Semaphore::new(max_concurrency));
                let mut tasks = FuturesUnordered::new();

                for table in &tables {
                    let table = table.clone();
                    let checkpoint = state.postgres.get(&table.name).cloned().unwrap_or_default();
                    let source = Arc::clone(&source);
                    let dest = Arc::clone(&dest);
                    let stats = stats.clone();
                    let state_handle = state_handle.clone();
                    let shutdown = shutdown.clone();
                    let run_id = run_id.clone();
                    let connection_id = connection.id.clone();
                    let semaphore = Arc::clone(&semaphore);
                    tasks.push(async move {
                        let permit = match semaphore.acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                return (
                                    table.name.clone(),
                                    Err(anyhow::anyhow!("failed to acquire concurrency permit")),
                                );
                            }
                        };
                        let _permit = permit;
                        let mut attempt = 0;
                        let mut backoff = Duration::from_millis(retry_backoff_ms);
                        let result = loop {
                            attempt += 1;
                            let attempt_result = source
                                .sync_table(TableSyncRequest {
                                    table: &table,
                                    dest: dest.as_ref(),
                                    checkpoint: checkpoint.clone(),
                                    state_handle: Some(state_handle.clone()),
                                    mode,
                                    dry_run,
                                    default_batch_size,
                                    schema_diff_enabled,
                                    stats: stats.clone(),
                                })
                                .await
                                .with_context(|| format!("syncing postgres table {}", table.name));
                            match attempt_result {
                                Ok(next_checkpoint) => break Ok(next_checkpoint),
                                Err(err) if attempt < max_retries => {
                                    telemetry::record_retry_attempt(&connection_id, "postgres_table");
                                    warn!(
                                        connection = %connection_id,
                                        run_id = run_id.as_deref().unwrap_or("none"),
                                        table = %table.name,
                                        attempt,
                                        backoff_ms = backoff.as_millis() as u64,
                                        last_synced_at = ?checkpoint.last_synced_at,
                                        last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                        error = %err,
                                        "postgres table sync attempt failed; retrying"
                                    );
                                    if wait_backoff(backoff, shutdown.clone()).await {
                                        info!(
                                            connection = %connection_id,
                                            run_id = run_id.as_deref().unwrap_or("none"),
                                            table = %table.name,
                                            attempt,
                                            "shutdown requested during postgres table retry backoff"
                                        );
                                        break Ok(
                                            load_latest_postgres_checkpoint(
                                                &state_handle,
                                                &table.name,
                                                &checkpoint,
                                            )
                                            .await,
                                        );
                                    }
                                    backoff = Duration::from_millis(backoff.as_millis() as u64 * 2);
                                    continue;
                                }
                                Err(err) => {
                                    error!(
                                        connection = %connection_id,
                                        run_id = run_id.as_deref().unwrap_or("none"),
                                        table = %table.name,
                                        attempt,
                                        last_synced_at = ?checkpoint.last_synced_at,
                                        last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                        error = %err,
                                        "postgres table sync attempt failed permanently"
                                    );
                                    break Err(err);
                                }
                            }
                        };
                        (table.name.clone(), result)
                    });
                }

                let mut first_error: Option<anyhow::Error> = None;
                while let Some((table_name, result)) = tasks.next().await {
                    match result {
                        Ok(checkpoint) => {
                            state.postgres.insert(table_name, checkpoint);
                        }
                        Err(err) => {
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                }

                if let Some(err) = first_error {
                    return Err(err);
                }
            }
        }
        SourceConfig::Salesforce(sf) => {
            if follow {
                anyhow::bail!("--follow is only supported for postgres CDC connections");
            }
            let source = SalesforceSource::new(sf.clone(), metadata.clone())?;
            let objects = source.resolve_objects().await?;
            let source = Arc::new(source);
            let dest = Arc::new(dest);
            let semaphore = Arc::new(Semaphore::new(max_concurrency));
            let mut tasks = FuturesUnordered::new();

            for object in &objects {
                let object = object.clone();
                let checkpoint = state
                    .salesforce
                    .get(&object.name)
                    .cloned()
                    .unwrap_or_default();
                let source = Arc::clone(&source);
                let dest = Arc::clone(&dest);
                let stats = stats.clone();
                let state_handle = state_handle.clone();
                let shutdown = shutdown.clone();
                let run_id = run_id.clone();
                let connection_id = connection.id.clone();
                let semaphore = Arc::clone(&semaphore);
                tasks.push(async move {
                    let permit = match semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            return (
                                object.name.clone(),
                                Err(anyhow::anyhow!("failed to acquire concurrency permit")),
                            );
                        }
                    };
                    let _permit = permit;
                    let mut attempt = 0;
                    let mut backoff = Duration::from_millis(retry_backoff_ms);
                    let result = loop {
                        attempt += 1;
                        let attempt_result = source
                            .sync_object(SalesforceSyncRequest {
                                object: &object,
                                dest: dest.as_ref(),
                                checkpoint: checkpoint.clone(),
                                state_handle: Some(state_handle.clone()),
                                mode,
                                dry_run,
                                stats: stats.clone(),
                            })
                            .await
                            .with_context(|| format!("syncing salesforce object {}", object.name));
                        match attempt_result {
                            Ok(next_checkpoint) => break Ok(next_checkpoint),
                            Err(err) if attempt < max_retries => {
                                telemetry::record_retry_attempt(&connection_id, "salesforce_object");
                                warn!(
                                    connection = %connection_id,
                                    run_id = run_id.as_deref().unwrap_or("none"),
                                    object = %object.name,
                                    attempt,
                                    backoff_ms = backoff.as_millis() as u64,
                                    last_synced_at = ?checkpoint.last_synced_at,
                                    last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                    error = %err,
                                    "salesforce object sync attempt failed; retrying"
                                );
                                if wait_backoff(backoff, shutdown.clone()).await {
                                    info!(
                                        connection = %connection_id,
                                        run_id = run_id.as_deref().unwrap_or("none"),
                                        object = %object.name,
                                        attempt,
                                        "shutdown requested during salesforce retry backoff"
                                    );
                                    break Ok(
                                        load_latest_salesforce_checkpoint(
                                            &state_handle,
                                            &object.name,
                                            &checkpoint,
                                        )
                                        .await,
                                    );
                                }
                                backoff = Duration::from_millis(backoff.as_millis() as u64 * 2);
                                continue;
                            }
                            Err(err) => {
                                error!(
                                    connection = %connection_id,
                                    run_id = run_id.as_deref().unwrap_or("none"),
                                    object = %object.name,
                                    attempt,
                                    last_synced_at = ?checkpoint.last_synced_at,
                                    last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                    error = %err,
                                    "salesforce object sync attempt failed permanently"
                                );
                                break Err(err);
                            }
                        }
                    };
                    (object.name.clone(), result)
                });
            }

            let mut first_error: Option<anyhow::Error> = None;
            while let Some((object_name, result)) = tasks.next().await {
                match result {
                    Ok(checkpoint) => {
                        state.salesforce.insert(object_name, checkpoint);
                    }
                    Err(err) => {
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            }

            if let Some(err) = first_error {
                return Err(err);
            }
        }
    }

    Ok(())
}

async fn cmd_run(config_path: PathBuf, connection_id: String) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;

    let connection = cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;

    let (shutdown_controller, shutdown_signal) = ShutdownController::new();
    let signal_controller = shutdown_controller.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_termination_signal().await;
        signal_controller.shutdown();
    });

    let result = match (&connection.source, &connection.destination) {
        (SourceConfig::Postgres(pg), DestinationConfig::BigQuery(_)) if pg.cdc.unwrap_or(true) => {
            cmd_sync(SyncCommandRequest {
                config_path,
                full: false,
                incremental: true,
                dry_run: false,
                connection_filter: Some(connection.id.clone()),
                schema_diff_enabled: false,
                follow: true,
                shutdown: Some(shutdown_signal.clone()),
            })
            .await
        }
        _ => {
            let interval = schedule_interval(connection)?;
            loop {
                if shutdown_signal.is_shutdown() {
                    break;
                }
                cmd_sync(SyncCommandRequest {
                    config_path: config_path.clone(),
                    full: false,
                    incremental: true,
                    dry_run: false,
                    connection_filter: Some(connection.id.clone()),
                    schema_diff_enabled: false,
                    follow: false,
                    shutdown: Some(shutdown_signal.clone()),
                })
                .await?;
                if wait_backoff(interval, Some(shutdown_signal.clone())).await {
                    break;
                }
            }
            Ok(())
        }
    };
    shutdown_controller.shutdown();
    signal_task.abort();
    result
}

async fn cmd_status(config_path: PathBuf, connection: Option<String>) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let state = SyncState::load(&cfg.state.path).await?;
    if let Some(connection_id) = connection {
        let connection_state = state.connections.get(&connection_id);
        let output = serde_json::to_string_pretty(&connection_state)?;
        println!("{}", output);
    } else {
        let output = serde_json::to_string_pretty(&state)?;
        println!("{}", output);
    }
    Ok(())
}

async fn cmd_validate(
    config_path: PathBuf,
    connection_filter: Option<String>,
    verbose: bool,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    let mut matched = false;
    for connection in &cfg.connections {
        if let Some(filter) = &connection_filter
            && &connection.id != filter
        {
            continue;
        }
        matched = true;

        if connection_filter.is_none() && !connection.enabled() {
            continue;
        }

        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
                if source.cdc_enabled() {
                    let tables = source.resolve_tables().await?;
                    source.validate_cdc_publication(&tables, verbose).await?;
                    info!(connection = %connection.id, "validated postgres CDC publication filters");
                } else {
                    info!(connection = %connection.id, "postgres CDC disabled; skipping publication validation");
                }
            }
            SourceConfig::Salesforce(_) => {
                info!(connection = %connection.id, "salesforce validation skipped");
            }
        }
    }

    if connection_filter.is_some() && !matched {
        anyhow::bail!("connection not found");
    }

    println!("Validation complete.");
    Ok(())
}

async fn cmd_report(
    config_path: PathBuf,
    connection_filter: Option<String>,
    limit: usize,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let state = SyncState::load(&cfg.state.path).await?;
    let state_value = if let Some(connection_id) = &connection_filter {
        serde_json::to_value(state.connections.get(connection_id))?
    } else {
        serde_json::to_value(&state)?
    };

    let recent_runs = if let Some(stats_cfg) = &cfg.stats {
        StatsDb::new(&stats_cfg.path)
            .await?
            .recent_runs(connection_filter.as_deref(), limit)
            .await?
    } else {
        Vec::new()
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&ReportOutput {
            state: state_value,
            recent_runs,
        })?
    );
    Ok(())
}

async fn cmd_reconcile(
    config_path: PathBuf,
    connection_id: String,
    table_filter: Option<String>,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    let connection = cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;

    let output = match (&connection.source, &connection.destination) {
        (SourceConfig::Postgres(pg), DestinationConfig::BigQuery(bq)) => {
            let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
            let dest = BigQueryDestination::new(bq.clone(), false, metadata.clone()).await?;
            let tables = source.resolve_tables().await?;
            let selected_tables: Vec<_> = if let Some(table_filter) = &table_filter {
                tables
                    .into_iter()
                    .filter(|table| &table.name == table_filter)
                    .collect()
            } else {
                tables
            };

            if selected_tables.is_empty() {
                anyhow::bail!("no matching tables selected for reconciliation");
            }

            let mut reports = Vec::with_capacity(selected_tables.len());
            for table in selected_tables {
                let destination_table = crate::types::destination_table_name(&table.name);
                let report = match (
                    source.summarize_table(&table).await,
                    dest.summarize_table(&destination_table).await,
                ) {
                    (Ok(source_summary), Ok(dest_summary)) => TableReconciliationReport {
                        source_table: table.name.clone(),
                        destination_table,
                        source_row_count: Some(source_summary.row_count),
                        destination_row_count: Some(dest_summary.row_count),
                        deleted_rows: Some(dest_summary.deleted_rows),
                        source_max_updated_at: source_summary.max_updated_at,
                        destination_max_synced_at: dest_summary.max_synced_at,
                        count_match: Some(reconcile_count_match(
                            &source_summary,
                            &dest_summary,
                            &table,
                        )),
                        error: None,
                    },
                    (Err(err), _) => TableReconciliationReport {
                        source_table: table.name.clone(),
                        destination_table,
                        source_row_count: None,
                        destination_row_count: None,
                        deleted_rows: None,
                        source_max_updated_at: None,
                        destination_max_synced_at: None,
                        count_match: None,
                        error: Some(format!("source summary failed: {err}")),
                    },
                    (_, Err(err)) => TableReconciliationReport {
                        source_table: table.name.clone(),
                        destination_table,
                        source_row_count: None,
                        destination_row_count: None,
                        deleted_rows: None,
                        source_max_updated_at: None,
                        destination_max_synced_at: None,
                        count_match: None,
                        error: Some(format!("destination summary failed: {err}")),
                    },
                };
                if let Some(count_match) = report.count_match {
                    telemetry::record_reconcile_table(&connection_id, count_match);
                }
                reports.push(report);
            }

            ReconciliationOutput {
                connection_id,
                tables: reports,
            }
        }
        _ => anyhow::bail!("reconcile currently supports postgres -> bigquery connections only"),
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn wait_backoff(duration: Duration, shutdown: Option<ShutdownSignal>) -> bool {
    if let Some(mut shutdown) = shutdown {
        tokio::select! {
            _ = tokio::time::sleep(duration) => false,
            changed = shutdown.changed() => changed,
        }
    } else {
        tokio::time::sleep(duration).await;
        false
    }
}

async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    #[test]
    fn reconcile_count_match_accounts_for_soft_deleted_rows() {
        let source = crate::sources::postgres::PostgresTableSummary {
            row_count: 10,
            max_updated_at: None,
        };
        let destination = crate::destinations::bigquery::DestinationTableSummary {
            row_count: 12,
            max_synced_at: None,
            deleted_rows: 2,
        };
        let table = crate::sources::postgres::ResolvedPostgresTable {
            name: "public.items".to_string(),
            primary_key: "id".to_string(),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: true,
            soft_delete_column: Some("deleted_at".to_string()),
            where_clause: None,
            columns: crate::config::ColumnSelection {
                include: Vec::new(),
                exclude: Vec::new(),
            },
        };

        assert!(reconcile_count_match(&source, &destination, &table));
    }

    #[test]
    fn reconcile_count_match_uses_raw_count_without_soft_delete() {
        let source = crate::sources::postgres::PostgresTableSummary {
            row_count: 10,
            max_updated_at: None,
        };
        let destination = crate::destinations::bigquery::DestinationTableSummary {
            row_count: 12,
            max_synced_at: None,
            deleted_rows: 2,
        };
        let table = crate::sources::postgres::ResolvedPostgresTable {
            name: "public.items".to_string(),
            primary_key: "id".to_string(),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: false,
            soft_delete_column: None,
            where_clause: None,
            columns: crate::config::ColumnSelection {
                include: Vec::new(),
                exclude: Vec::new(),
            },
        };

        assert!(!reconcile_count_match(&source, &destination, &table));
    }
}

#[cfg(test)]
mod sync_selection_tests {
    use super::*;

    fn postgres_connection(id: &str, enabled: Option<bool>) -> crate::config::ConnectionConfig {
        crate::config::ConnectionConfig {
            id: id.to_string(),
            enabled,
            source: crate::config::SourceConfig::Postgres(crate::config::PostgresConfig {
                url: "postgres://localhost/test".to_string(),
                tables: Some(vec![crate::config::PostgresTableConfig {
                    name: "public.accounts".to_string(),
                    primary_key: Some("id".to_string()),
                    updated_at_column: Some("updated_at".to_string()),
                    soft_delete: Some(false),
                    soft_delete_column: None,
                    where_clause: None,
                    columns: None,
                }]),
                table_selection: None,
                batch_size: Some(1000),
                cdc: Some(false),
                publication: None,
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: None,
                cdc_batch_size: None,
                cdc_max_fill_ms: None,
                cdc_max_pending_events: None,
                cdc_idle_timeout_seconds: None,
                cdc_tls: None,
                cdc_tls_ca_path: None,
                cdc_tls_ca: None,
            }),
            destination: crate::config::DestinationConfig::BigQuery(
                crate::config::BigQueryConfig {
                    project_id: "project".to_string(),
                    dataset: "dataset".to_string(),
                    location: Some("US".to_string()),
                    service_account_key_path: None,
                    service_account_key: None,
                    partition_by_synced_at: Some(false),
                    storage_write_enabled: Some(false),
                    emulator_http: Some("http://localhost:9050".to_string()),
                    emulator_grpc: Some("localhost:9051".to_string()),
                },
            ),
            schedule: None,
        }
    }

    #[test]
    fn select_sync_connections_errors_when_filter_missing() {
        let connections = vec![postgres_connection("app", Some(true))];

        let error = select_sync_connections(&connections, Some("missing"))
            .expect_err("missing connection should fail");

        assert!(error.to_string().contains("connection not found"));
    }

    #[test]
    fn select_sync_connections_errors_when_filtered_connection_is_disabled() {
        let connections = vec![postgres_connection("app", Some(false))];

        let error = select_sync_connections(&connections, Some("app"))
            .expect_err("disabled filtered connection should fail");

        assert!(error.to_string().contains("disabled"));
    }

    #[test]
    fn select_sync_connections_returns_only_enabled_connections_without_filter() {
        let connections = vec![
            postgres_connection("enabled", Some(true)),
            postgres_connection("disabled", Some(false)),
            postgres_connection("default_enabled", None),
        ];

        let selected = select_sync_connections(&connections, None).expect("selection succeeds");
        let ids: Vec<&str> = selected
            .iter()
            .map(|connection| connection.id.as_str())
            .collect();

        assert_eq!(ids, vec!["enabled", "default_enabled"]);
    }

    #[tokio::test]
    async fn load_latest_postgres_checkpoint_prefers_persisted_progress() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.db");
        let store = SyncStateStore::open(&path).await?;
        let handle = store.handle("app");
        let persisted = TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            ..Default::default()
        };
        handle
            .save_postgres_checkpoint("public.accounts", &persisted)
            .await?;

        let fallback = TableCheckpoint {
            last_primary_key: Some("1".to_string()),
            ..Default::default()
        };
        let loaded = load_latest_postgres_checkpoint(&handle, "public.accounts", &fallback).await;

        assert_eq!(loaded.last_primary_key.as_deref(), Some("42"));
        Ok(())
    }

    #[tokio::test]
    async fn load_latest_salesforce_checkpoint_prefers_persisted_progress() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.db");
        let store = SyncStateStore::open(&path).await?;
        let handle = store.handle("app");
        let persisted = TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            ..Default::default()
        };
        handle
            .save_salesforce_checkpoint("Account", &persisted)
            .await?;

        let fallback = TableCheckpoint {
            last_primary_key: Some("1".to_string()),
            ..Default::default()
        };
        let loaded = load_latest_salesforce_checkpoint(&handle, "Account", &fallback).await;

        assert_eq!(loaded.last_primary_key.as_deref(), Some("42"));
        Ok(())
    }
}
