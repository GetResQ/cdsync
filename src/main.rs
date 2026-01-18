mod config;
mod destinations;
mod sources;
mod state;
mod stats;
mod types;

use crate::config::{Config, DestinationConfig, LoggingConfig, SourceConfig};
use crate::destinations::bigquery::BigQueryDestination;
use crate::sources::postgres::PostgresSource;
use crate::sources::salesforce::SalesforceSource;
use crate::state::{ConnectionState, SyncState};
use crate::stats::{StatsDb, StatsHandle};
use crate::types::SyncMode;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

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
}

#[tokio::main]
async fn main() -> Result<()> {
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
        } => cmd_sync(config, full, incremental, dry_run, connection, schema_diff).await,
        Commands::Status { config, connection } => cmd_status(config, connection).await,
        Commands::Validate {
            config,
            connection,
            verbose,
        } => cmd_validate(config, connection, verbose).await,
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
    init_logging(cfg.logging.as_ref());

    for connection in &cfg.connections {
        if !connection.enabled() {
            continue;
        }
        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone()).await?;
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
                let source = SalesforceSource::new(sf.clone())?;
                source.validate().await?;
            }
        }

        match &connection.destination {
            DestinationConfig::BigQuery(bq) => {
                let dest = BigQueryDestination::new(bq.clone(), false).await?;
                dest.validate().await?;
            }
        }
    }

    println!("Config validation complete.");
    Ok(())
}

async fn cmd_sync(
    config_path: PathBuf,
    full: bool,
    incremental: bool,
    dry_run: bool,
    connection_filter: Option<String>,
    schema_diff_enabled: bool,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    init_logging(cfg.logging.as_ref());

    let mode = match (full, incremental) {
        (true, false) => SyncMode::Full,
        (false, true) => SyncMode::Incremental,
        (false, false) => SyncMode::Incremental,
        (true, true) => {
            anyhow::bail!("--full and --incremental are mutually exclusive");
        }
    };

    let mut state = SyncState::load(&cfg.state.path).await?;
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
    let max_retries = cfg
        .sync
        .as_ref()
        .and_then(|s| s.max_retries)
        .unwrap_or(3);
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

    for connection in &cfg.connections {
        if !connection.enabled() {
            continue;
        }
        if let Some(filter) = &connection_filter {
            if &connection.id != filter {
                continue;
            }
        }

        let connection_state = state
            .connections
            .entry(connection.id.clone())
            .or_insert_with(ConnectionState::default);
        connection_state.last_sync_started_at = Some(Utc::now());
        connection_state.last_sync_status = Some("running".to_string());
        connection_state.last_error = None;

        let stats_handle = stats_db
            .as_ref()
            .map(|_| StatsHandle::new(&connection.id));

        let result = sync_connection(
            connection,
            connection_state,
            mode,
            dry_run,
            default_batch_size,
            max_retries,
            max_concurrency,
            retry_backoff_ms,
            schema_diff_enabled,
            stats_handle.clone(),
        )
        .await;

        connection_state.last_sync_finished_at = Some(Utc::now());
        let error_string = result.as_ref().err().map(|err| err.to_string());
        match &result {
            Ok(_) => {
                connection_state.last_sync_status = Some("success".to_string());
            }
            Err(_) => {
                connection_state.last_sync_status = Some("failed".to_string());
                connection_state.last_error = error_string.clone();
            }
        }
        if let Some(handle) = &stats_handle {
            let status = if result.is_ok() { "success" } else { "failed" };
            handle.finish(status, error_string).await;
            if let Some(db) = &stats_db {
                db.persist_run(handle).await?;
            }
        }
        state.save(&cfg.state.path).await?;

        if let Err(err) = result {
            return Err(err);
        }
    }

    println!("Sync complete.");
    Ok(())
}

async fn sync_connection(
    connection: &crate::config::ConnectionConfig,
    state: &mut ConnectionState,
    mode: SyncMode,
    dry_run: bool,
    default_batch_size: usize,
    max_retries: u32,
    max_concurrency: usize,
    retry_backoff_ms: u64,
    schema_diff_enabled: bool,
    stats: Option<StatsHandle>,
) -> Result<()> {
    let dest = match &connection.destination {
        DestinationConfig::BigQuery(bq) => BigQueryDestination::new(bq.clone(), dry_run).await?,
    };
    dest.validate().await?;

    match &connection.source {
        SourceConfig::Postgres(pg) => {
            let source = PostgresSource::new(pg.clone()).await?;
            let tables = source.resolve_tables().await?;
            if source.cdc_enabled() {
                info!("syncing postgres via CDC");
                let mut attempt = 0;
                let mut backoff = Duration::from_millis(retry_backoff_ms);
                loop {
                    attempt += 1;
                    let result = source
                        .sync_cdc(
                            &dest,
                            state,
                            mode,
                            dry_run,
                            default_batch_size,
                            &tables,
                            schema_diff_enabled,
                            stats.clone(),
                        )
                        .await
                        .with_context(|| "syncing postgres CDC");
                    match result {
                        Ok(_) => break,
                        Err(err) if attempt < max_retries => {
                            tokio::time::sleep(backoff).await;
                            backoff = Duration::from_millis(backoff.as_millis() as u64 * 2);
                            continue;
                        }
                        Err(err) => return Err(err),
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
                    let checkpoint = state
                        .postgres
                        .get(&table.name)
                        .cloned()
                        .unwrap_or_default();
                    let source = Arc::clone(&source);
                    let dest = Arc::clone(&dest);
                    let stats = stats.clone();
                    let semaphore = Arc::clone(&semaphore);
                    tasks.push(async move {
                        let permit = match semaphore.acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                return (
                                    table.name.clone(),
                                    Err(anyhow::anyhow!(
                                        "failed to acquire concurrency permit"
                                    )),
                                );
                            }
                        };
                        let _permit = permit;
                        let mut attempt = 0;
                        let mut backoff = Duration::from_millis(retry_backoff_ms);
                        let result = loop {
                            attempt += 1;
                            let attempt_result = source
                                .sync_table(
                                    &table,
                                    dest.as_ref(),
                                    checkpoint.clone(),
                                    mode,
                                    dry_run,
                                    default_batch_size,
                                    schema_diff_enabled,
                                    stats.clone(),
                                )
                                .await
                                .with_context(|| format!("syncing postgres table {}", table.name));
                            match attempt_result {
                                Ok(checkpoint) => break Ok(checkpoint),
                                Err(err) if attempt < max_retries => {
                                    tokio::time::sleep(backoff).await;
                                    backoff =
                                        Duration::from_millis(backoff.as_millis() as u64 * 2);
                                    continue;
                                }
                                Err(err) => break Err(err),
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
            let source = SalesforceSource::new(sf.clone())?;
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
                            .sync_object(
                                &object,
                                dest.as_ref(),
                                checkpoint.clone(),
                                mode,
                                dry_run,
                                stats.clone(),
                            )
                            .await
                            .with_context(|| {
                                format!("syncing salesforce object {}", object.name)
                            });
                        match attempt_result {
                            Ok(checkpoint) => break Ok(checkpoint),
                            Err(err) if attempt < max_retries => {
                                tokio::time::sleep(backoff).await;
                                backoff =
                                    Duration::from_millis(backoff.as_millis() as u64 * 2);
                                continue;
                            }
                            Err(err) => break Err(err),
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

async fn cmd_status(config_path: PathBuf, connection: Option<String>) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
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
    init_logging(cfg.logging.as_ref());

    let mut matched = false;
    for connection in &cfg.connections {
        if let Some(filter) = &connection_filter {
            if &connection.id != filter {
                continue;
            }
        }
        matched = true;

        if connection_filter.is_none() && !connection.enabled() {
            continue;
        }

        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone()).await?;
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

fn init_logging(logging: Option<&LoggingConfig>) {
    let level = logging.and_then(|l| l.level.clone()).unwrap_or_else(|| "info".to_string());
    let json = logging.and_then(|l| l.json).unwrap_or(false);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    if json {
        tracing_subscriber::fmt().with_env_filter(filter).json().init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}
