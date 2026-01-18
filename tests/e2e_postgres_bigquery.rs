use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::PostgresSource;
use cdsync::types::TableCheckpoint;
use cdsync::types::{destination_table_name, SyncMode};
use sqlx::postgres::PgPoolOptions;
use std::env;
use uuid::Uuid;
use url::Url;

#[tokio::test]
#[ignore]
async fn e2e_postgres_bigquery_full_refresh() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http_raw = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc_raw = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id =
        env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset =
        env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let bq_http = normalize_http(&bq_http_raw)?;
    let bq_grpc = normalize_grpc(&bq_grpc_raw)?;

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_e2e_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text, updated_at timestamptz default now())",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, 'alpha'), (2, 'beta')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
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
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_max_fill_ms: None,
        cdc_max_pending_events: None,
        cdc_idle_timeout_seconds: None,
        cdc_tls: None,
        cdc_tls_ca_path: None,
        cdc_tls_ca: None,
    };

    let bq_config = BigQueryConfig {
        project_id: project_id.clone(),
        dataset: dataset.clone(),
        location: Some("US".to_string()),
        service_account_key_path: None,
        service_account_key: None,
        partition_by_synced_at: Some(false),
        emulator_http: Some(bq_http.clone()),
        emulator_grpc: Some(bq_grpc.clone()),
    };

    let source = PostgresSource::new(pg_config).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false).await?;
    dest.validate().await?;

    for table in &tables {
        source
            .sync_table(
                table,
                &dest,
                TableCheckpoint::default(),
                SyncMode::Full,
                false,
                1000,
                false,
                None,
            )
            .await?;
    }

    let http_client = reqwest::Client::new();
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}/data",
        bq_http, project_id, dataset, dest_table
    );
    let response = http_client.get(url).send().await?;
    let payload: serde_json::Value = response.json().await?;
    let total_rows = match payload.get("totalRows") {
        Some(serde_json::Value::String(value)) => value.parse::<u64>().ok(),
        Some(serde_json::Value::Number(value)) => value.as_u64(),
        _ => None,
    }
    .context("missing totalRows in emulator response")?;

    assert_eq!(total_rows, 2);
    Ok(())
}

fn normalize_http(raw: &str) -> Result<String> {
    if raw.contains("://") {
        Ok(raw.to_string())
    } else {
        Ok(format!("http://{raw}"))
    }
}

fn normalize_grpc(raw: &str) -> Result<String> {
    if raw.contains("://") {
        let url = Url::parse(raw).context("invalid CDSYNC_E2E_BQ_GRPC url")?;
        let host = url.host_str().context("CDSYNC_E2E_BQ_GRPC missing host")?;
        let port = url.port().unwrap_or(default_port(url.scheme()));
        Ok(format!("{host}:{port}"))
    } else {
        Ok(raw.to_string())
    }
}

fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}
