#![allow(dead_code)]

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;

pub mod real_bigquery {
    use anyhow::{Context, Result};
    use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
    use gcloud_bigquery::client::{Client, ClientConfig};
    use gcloud_bigquery::http::job::query::QueryRequest;
    use gcloud_bigquery::query::row::Row as BigQueryRow;
    use std::env;
    use std::path::{Path, PathBuf};
    use std::sync::Once;

    static RUSTLS_PROVIDER: Once = Once::new();

    pub struct RealBigQueryEnv {
        pub project_id: String,
        pub dataset: String,
        pub location: String,
        pub key_path: PathBuf,
    }

    pub fn install_rustls_provider() {
        RUSTLS_PROVIDER.call_once(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("installing rustls ring provider");
        });
    }

    pub fn load_env() -> Result<RealBigQueryEnv> {
        let project_id =
            env::var("CDSYNC_REAL_BQ_PROJECT").unwrap_or_else(|_| "resq-develop".to_string());
        let dataset =
            env::var("CDSYNC_REAL_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e_real".to_string());
        let location = env::var("CDSYNC_REAL_BQ_LOCATION").unwrap_or_else(|_| "US".to_string());
        let key_path = env::var("CDSYNC_REAL_BQ_KEY_PATH")
            .unwrap_or_else(|_| ".secrets/resq-develop-f0c7ed0c3d65.json".to_string());
        anyhow::ensure!(
            Path::new(&key_path).exists(),
            "set CDSYNC_REAL_BQ_KEY_PATH or place the key in .secrets/"
        );
        Ok(RealBigQueryEnv {
            project_id,
            dataset,
            location,
            key_path: PathBuf::from(key_path),
        })
    }

    pub async fn client(key_path: &Path) -> Result<Client> {
        let key = CredentialsFile::new_from_file(key_path.to_string_lossy().to_string()).await?;
        let (config, _project) = ClientConfig::new_with_credentials(key).await?;
        Ok(Client::new(config).await?)
    }

    pub async fn fetch_live_table_fields(
        client: &Client,
        project_id: &str,
        dataset: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let table = client.table().get(project_id, dataset, table).await?;
        let schema = table.schema.context("missing table schema")?;
        Ok(schema.fields.into_iter().map(|field| field.name).collect())
    }

    pub async fn query_i64(
        client: &Client,
        project_id: &str,
        location: &str,
        sql: &str,
    ) -> Result<i64> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: location.to_string(),
            ..Default::default()
        };
        let mut iter = client.query::<BigQueryRow>(project_id, request).await?;
        let row = iter.next().await?.context("missing query row")?;
        row.column::<i64>(0)
            .map_err(|err| anyhow::anyhow!("decoding query result failed: {}", err))
    }
}

pub async fn fetch_table_fields(
    client: &Client,
    base_url: &str,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}",
        base_url, project, dataset, table
    );
    let payload: Value = client.get(url).send().await?.json().await?;
    let fields = payload
        .pointer("/schema/fields")
        .and_then(|v| v.as_array())
        .context("missing schema.fields in emulator response")?;
    fields
        .iter()
        .map(|field| {
            field
                .get("name")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
                .context("schema field missing name")
        })
        .collect()
}

pub async fn fetch_table_rows(
    client: &Client,
    base_url: &str,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<Vec<Value>>> {
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}/data",
        base_url, project, dataset, table
    );
    let payload: Value = client.get(url).send().await?.json().await?;
    let rows = payload
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let cells = row
            .get("f")
            .and_then(|v| v.as_array())
            .context("row missing f array")?;
        let values = cells
            .iter()
            .map(|cell| cell.get("v").cloned().unwrap_or(Value::Null))
            .collect();
        output.push(values);
    }
    Ok(output)
}

pub fn map_rows(fields: &[String], rows: Vec<Vec<Value>>) -> Result<Vec<HashMap<String, Value>>> {
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != fields.len() {
            anyhow::bail!(
                "row length {} does not match field length {}",
                row.len(),
                fields.len()
            );
        }
        let mut map = HashMap::new();
        for (idx, field) in fields.iter().enumerate() {
            map.insert(field.clone(), row[idx].clone());
        }
        output.push(map);
    }
    Ok(output)
}

pub fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

pub async fn delete_table_if_exists(
    client: &Client,
    base_url: &str,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}",
        base_url, project, dataset, table
    );
    let response = client.delete(url).send().await?;
    if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
        Ok(())
    } else {
        anyhow::bail!("failed to delete table {}: {}", table, response.status());
    }
}
