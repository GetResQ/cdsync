use crate::config::BigQueryConfig;
use crate::destinations::{Destination, WriteMode, with_metadata_schema};
use crate::tls;
use crate::types::{ColumnSchema, DataType, MetadataColumns, TableSchema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use futures::StreamExt;
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::google_cloud_auth::project::Config as GoogleAuthConfig;
use gcloud_bigquery::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::error::Error as BqError;
use gcloud_bigquery::http::job::get::GetJobRequest;
use gcloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use gcloud_bigquery::http::job::{
    CreateDisposition, Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState,
    JobType, WriteDisposition,
};
use gcloud_bigquery::http::table::{
    ParquetOptions, SourceFormat, Table, TableFieldSchema, TableFieldType, TableReference,
    TableSchema as BqTableSchema, TimePartitionType, TimePartitioning,
};
use gcloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use gcloud_bigquery::http::tabledata::list::{Tuple as BqTuple, Value as BqValue};
use gcloud_bigquery::storage_write::AppendRowsRequestBuilder;
use gcloud_bigquery::storage_write::stream::committed::CommittedStream;
use gcloud_googleapis::cloud::bigquery::storage::v1::AppendRowsResponse;
use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response as AppendRowsStreamResponse;
use polars::frame::row::Row as PolarsRow;
use polars::io::parquet::write::{ParquetCompression, ParquetWriter};
use polars::prelude::{
    AnyValue, BinaryChunked, DataFrame, IntoSeries, NamedFrom, NewChunkedArray, Series,
};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value as ReflectValue};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use token_source::{TokenSource, TokenSourceProvider};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::Code;
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
pub struct BigQueryDestination {
    client: Client,
    gcs_token_source: Option<Arc<dyn TokenSource>>,
    config: BigQueryConfig,
    dry_run: bool,
    metadata: MetadataColumns,
    storage_writers: Arc<Mutex<HashMap<String, Arc<StorageWriteTableWriter>>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationTableSummary {
    pub row_count: i64,
    pub max_synced_at: Option<DateTime<Utc>>,
    pub deleted_rows: i64,
}

struct StorageWriteTableWriter {
    stream: Arc<CommittedStream>,
    next_offset: Mutex<i64>,
    schema_key: String,
    descriptor_proto: DescriptorProto,
    message_descriptor: MessageDescriptor,
}

#[derive(Clone, Copy)]
enum BatchLoadFormat {
    Ndjson,
    Parquet,
}

impl BigQueryDestination {
    pub async fn new(
        mut config: BigQueryConfig,
        dry_run: bool,
        metadata: MetadataColumns,
    ) -> Result<Self> {
        tls::install_rustls_provider();
        if config.emulator_http.is_none() && config.emulator_grpc.is_some() {
            anyhow::bail!("bigquery.emulator_grpc requires emulator_http");
        }

        let (client_config, gcs_token_source, _project_from_auth) = if let Some(raw_http) =
            &config.emulator_http
        {
            let emulator_http = if raw_http.contains("://") {
                raw_http.to_string()
            } else {
                format!("http://{raw_http}")
            };

            let emulator_grpc = if let Some(raw_grpc) = &config.emulator_grpc {
                if raw_grpc.contains("://") {
                    let url = Url::parse(raw_grpc).context("invalid bigquery.emulator_grpc url")?;
                    let host = url
                        .host_str()
                        .context("bigquery.emulator_grpc missing host")?;
                    let port = url.port().unwrap_or(default_port(url.scheme()));
                    format!("{host}:{port}")
                } else {
                    raw_grpc.to_string()
                }
            } else {
                let url =
                    Url::parse(&emulator_http).context("invalid bigquery.emulator_http url")?;
                let host = url
                    .host_str()
                    .context("bigquery.emulator_http missing host")?;
                let port = url.port().unwrap_or(default_port(url.scheme()));
                format!("{host}:{port}")
            };

            config.emulator_http = Some(emulator_http.clone());
            config.emulator_grpc = Some(emulator_grpc.clone());

            (
                ClientConfig::new_with_emulator(&emulator_grpc, emulator_http),
                None,
                None,
            )
        } else if let Some(path) = &config.service_account_key_path {
            let key = CredentialsFile::new_from_file(path.to_string_lossy().to_string()).await?;
            let (bq_config, project) = ClientConfig::new_with_credentials(key.clone()).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(key),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        } else if let Some(raw_key) = &config.service_account_key {
            let key = CredentialsFile::new_from_str(raw_key).await?;
            let (bq_config, project) = ClientConfig::new_with_credentials(key).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(CredentialsFile::new_from_str(raw_key).await?),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        } else {
            let (bq_config, project) = ClientConfig::new_with_auth().await?;
            let token_source = DefaultTokenSourceProvider::new(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        };
        let client = Client::new(client_config).await?;
        Ok(Self {
            client,
            gcs_token_source,
            config,
            dry_run,
            metadata,
            storage_writers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn validate(&self) -> Result<()> {
        if self.dry_run {
            info!("dry-run: skipping BigQuery validation");
            return Ok(());
        }
        if self.config.emulator_http.is_some() {
            info!("bigquery emulator: skipping validation");
            return Ok(());
        }
        match self
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
            .await
        {
            Ok(_) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn batch_load_enabled(&self) -> bool {
        self.config.batch_load_bucket.is_some() && self.config.emulator_http.is_none()
    }

    pub async fn summarize_table(&self, table_id: &str) -> Result<DestinationTableSummary> {
        let sql = format!(
            "SELECT COUNT(1) AS row_count, \
             MAX(`{synced}`) AS max_synced_at, \
             COUNTIF(`{deleted}` IS NOT NULL) AS deleted_rows \
             FROM `{project}.{dataset}.{table}`",
            synced = self.metadata.synced_at,
            deleted = self.metadata.deleted_at,
            project = self.config.project_id,
            dataset = self.config.dataset,
            table = table_id
        );
        let response = self.run_query(&sql).await?;
        let row = response
            .rows
            .unwrap_or_default()
            .into_iter()
            .next()
            .context("reconciliation query returned no rows")?;
        Ok(DestinationTableSummary {
            row_count: tuple_value_as_i64(&row, 0)?,
            max_synced_at: tuple_value_as_datetime(&row, 1)?,
            deleted_rows: tuple_value_as_i64(&row, 2)?,
        })
    }

    async fn ensure_dataset(&self) -> Result<()> {
        if self.dry_run {
            info!("dry-run: ensure dataset {}", self.config.dataset);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/datasets/{}",
                emulator_http, self.config.project_id, self.config.dataset
            );
            let client = reqwest::Client::new();
            let response = client.get(&url).send().await?;
            if response.status().is_success() {
                return Ok(());
            }
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                let create_url = format!(
                    "{}/projects/{}/datasets",
                    emulator_http, self.config.project_id
                );
                let body = json!({
                    "datasetReference": {
                        "projectId": self.config.project_id,
                        "datasetId": self.config.dataset
                    }
                });
                let response = client.post(create_url).json(&body).send().await?;
                if response.status().is_success() {
                    return Ok(());
                }
                anyhow::bail!(
                    "failed to create dataset in emulator: {}",
                    response.status()
                );
            }
            anyhow::bail!(
                "failed to fetch dataset from emulator: {}",
                response.status()
            );
        }

        match self
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
            .await
        {
            Ok(_) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => {
                let location = self
                    .config
                    .location
                    .clone()
                    .context("dataset not found; location required to create dataset")?;
                let mut dataset = gcloud_bigquery::http::dataset::Dataset::default();
                dataset.dataset_reference = gcloud_bigquery::http::dataset::DatasetReference {
                    project_id: self.config.project_id.clone(),
                    dataset_id: self.config.dataset.clone(),
                };
                dataset.location = location;
                self.client.dataset().create(&dataset).await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn run_query(&self, sql: &str) -> Result<QueryResponse> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: self.config.location.clone().unwrap_or_default(),
            ..Default::default()
        };

        let response = if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/queries",
                emulator_http, self.config.project_id
            );
            reqwest::Client::new()
                .post(url)
                .json(&request)
                .send()
                .await?
                .json::<QueryResponse>()
                .await?
        } else {
            self.client
                .job()
                .query(&self.config.project_id, &request)
                .await?
        };

        if let Some(errors) = &response.errors
            && !errors.is_empty()
        {
            error!(errors = ?errors, "BigQuery query returned errors");
            anyhow::bail!("BigQuery query returned errors: {:?}", errors);
        }

        Ok(response)
    }

    fn storage_write_enabled(&self) -> bool {
        self.config.storage_write_enabled.unwrap_or(true) && self.config.emulator_http.is_none()
    }

    async fn append_rows_via_storage_write(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
    ) -> Result<bool> {
        if !self.storage_write_enabled() {
            return Ok(false);
        }

        let full_schema = with_metadata_schema(schema, &self.metadata);
        if !supports_storage_write_schema(&full_schema) {
            warn!(
                table = table_id,
                "storage write disabled for schema; falling back to insertAll"
            );
            return Ok(false);
        }

        let writer = self
            .get_or_create_storage_writer(table_id, &full_schema)
            .await?;
        let rows = encode_storage_write_rows(frame, &full_schema, &writer.message_descriptor)?;
        if rows.is_empty() {
            return Ok(true);
        }

        let mut offset_guard = writer.next_offset.lock().await;
        let offset = *offset_guard;
        let batch_len = rows.len() as i64;
        let request = AppendRowsRequestBuilder::new(writer.descriptor_proto.clone(), rows)
            .with_offset(offset);
        let append_result = writer.stream.append_rows(vec![request]).await;
        let mut responses = match append_result {
            Ok(responses) => responses,
            Err(err) if err.code() == Code::AlreadyExists => {
                *offset_guard += batch_len;
                return Ok(true);
            }
            Err(err) => {
                error!(
                    table = %table_id,
                    rows = batch_len,
                    error = %err,
                    "BigQuery Storage Write append failed"
                );
                return Err(anyhow::anyhow!("storage write append failed: {}", err));
            }
        };

        while let Some(response) = responses.next().await {
            let response = response.map_err(|err| {
                error!(
                    table = %table_id,
                    rows = batch_len,
                    error = %err,
                    "BigQuery Storage Write response failed"
                );
                anyhow::anyhow!("storage write response failed: {}", err)
            })?;
            if let Some(advance_offset) = validate_storage_write_response(table_id, &response)?
                && advance_offset
            {
                *offset_guard += batch_len;
                return Ok(true);
            }
        }

        *offset_guard += batch_len;
        Ok(true)
    }

    async fn get_or_create_storage_writer(
        &self,
        table_id: &str,
        schema: &TableSchema,
    ) -> Result<Arc<StorageWriteTableWriter>> {
        let schema_key = storage_write_schema_key(schema);
        let mut guard = self.storage_writers.lock().await;
        if let Some(existing) = guard.get(table_id)
            && existing.schema_key == schema_key
        {
            return Ok(existing.clone());
        }

        let descriptor_proto = build_storage_write_descriptor(schema);
        let message_descriptor = build_message_descriptor(&descriptor_proto)?;
        let fqtn = format!(
            "projects/{}/datasets/{}/tables/{}",
            self.config.project_id, self.config.dataset, table_id
        );
        let stream = self
            .client
            .committed_storage_writer()
            .create_write_stream(&fqtn)
            .await
            .map_err(|err| anyhow::anyhow!("creating storage write stream failed: {}", err))?;
        let writer = Arc::new(StorageWriteTableWriter {
            stream: Arc::new(stream),
            next_offset: Mutex::new(0),
            schema_key,
            descriptor_proto,
            message_descriptor,
        });
        guard.insert(table_id.to_string(), writer.clone());
        Ok(writer)
    }

    async fn invalidate_storage_writer(&self, table_id: &str) {
        let mut guard = self.storage_writers.lock().await;
        guard.remove(table_id);
    }

    async fn ensure_table_internal(
        &self,
        schema: &TableSchema,
        table_id: &str,
        with_partition: bool,
    ) -> Result<()> {
        self.ensure_dataset().await?;
        let schema = with_metadata_schema(schema, &self.metadata);
        let desired_fields = bq_fields_from_schema(&schema.columns);
        let bq_schema = BqTableSchema {
            fields: desired_fields.clone(),
        };

        if self.dry_run {
            info!("dry-run: ensure table {}", table_id);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}",
                emulator_http, self.config.project_id, self.config.dataset, table_id
            );
            let response = client.get(&url).send().await?;
            if response.status().is_success() {
                return Ok(());
            }
            if response.status() != reqwest::StatusCode::NOT_FOUND {
                anyhow::bail!("emulator table lookup failed: {}", response.status());
            }
            let mut body = json!({
                "tableReference": {
                    "projectId": self.config.project_id,
                    "datasetId": self.config.dataset,
                    "tableId": table_id
                },
                "schema": bq_schema
            });
            if with_partition && self.config.partition_by_synced_at.unwrap_or(false) {
                body["timePartitioning"] = json!({
                    "type": "DAY",
                    "field": self.metadata.synced_at
                });
            }
            let create_url = format!(
                "{}/projects/{}/datasets/{}/tables",
                emulator_http, self.config.project_id, self.config.dataset
            );
            let response = client.post(create_url).json(&body).send().await?;
            if response.status().is_success() {
                return Ok(());
            }
            anyhow::bail!("emulator table create failed: {}", response.status());
        }

        match self
            .client
            .table()
            .get(&self.config.project_id, &self.config.dataset, table_id)
            .await
        {
            Ok(mut table) => {
                let existing_fields = table
                    .schema
                    .as_ref()
                    .map(|s| s.fields.clone())
                    .unwrap_or_default();
                let existing_names: HashSet<String> = existing_fields
                    .iter()
                    .map(|f| f.name.to_lowercase())
                    .collect();

                let mut updated_fields = existing_fields;
                for field in desired_fields.clone() {
                    if !existing_names.contains(&field.name.to_lowercase()) {
                        updated_fields.push(field);
                    }
                }

                if updated_fields.len() != existing_names.len() {
                    table.schema = Some(BqTableSchema {
                        fields: updated_fields,
                    });
                    self.client.table().patch(&table).await?;
                }
                Ok(())
            }
            Err(BqError::Response(err)) if err.code == 404 => {
                let table = Table {
                    table_reference: TableReference {
                        project_id: self.config.project_id.clone(),
                        dataset_id: self.config.dataset.clone(),
                        table_id: table_id.to_string(),
                    },
                    time_partitioning: if with_partition
                        && self.config.partition_by_synced_at.unwrap_or(false)
                    {
                        Some(TimePartitioning {
                            partition_type: TimePartitionType::Day,
                            expiration_ms: None,
                            field: Some(self.metadata.synced_at.clone()),
                        })
                    } else {
                        None
                    },
                    schema: Some(bq_schema),
                    ..Default::default()
                };
                self.client.table().create(&table).await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn append_rows(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
        primary_key: Option<&str>,
    ) -> Result<()> {
        if frame.height() == 0 {
            return Ok(());
        }
        if self.dry_run {
            info!("dry-run: insert {} rows into {}", frame.height(), table_id);
            return Ok(());
        }
        if self
            .append_rows_via_batch_load(table_id, schema, frame)
            .await?
        {
            return Ok(());
        }
        if self
            .append_rows_via_storage_write(table_id, schema, frame)
            .await?
        {
            return Ok(());
        }

        let rows = dataframe_to_json_rows(frame)?;
        let mut request: InsertAllRequest<Map<String, Value>> = InsertAllRequest::default();
        for row in rows {
            let insert_id = primary_key
                .and_then(|pk| row.get(pk))
                .and_then(value_to_insert_id);
            request.rows.push(Row {
                insert_id,
                json: row,
            });
        }

        if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}/insertAll",
                emulator_http, self.config.project_id, self.config.dataset, table_id
            );
            let response = reqwest::Client::new()
                .post(url)
                .json(&request)
                .send()
                .await?;
            if !response.status().is_success() {
                error!(
                    table = %table_id,
                    status = %response.status(),
                    rows = frame.height(),
                    "BigQuery emulator insert request failed"
                );
                anyhow::bail!("emulator insert failed: {}", response.status());
            }
            let payload: serde_json::Value = response.json().await?;
            if let Some(errors) = payload.get("insertErrors") {
                let row_errors = errors.as_array().map(|rows| rows.len()).unwrap_or(1) as u64;
                crate::telemetry::record_bigquery_row_errors(table_id, row_errors);
                error!(
                    table = %table_id,
                    row_errors,
                    rows = frame.height(),
                    "BigQuery emulator insert returned row errors"
                );
                anyhow::bail!(
                    "BigQuery emulator insert errors for {}: {}",
                    table_id,
                    errors
                );
            }
            return Ok(());
        }

        let response = self
            .client
            .tabledata()
            .insert(
                &self.config.project_id,
                &self.config.dataset,
                table_id,
                &request,
            )
            .await?;
        if let Some(errors) = response.insert_errors {
            crate::telemetry::record_bigquery_row_errors(table_id, errors.len() as u64);
            error!(
                table = %table_id,
                row_errors = errors.len(),
                rows = frame.height(),
                "BigQuery insertAll returned row errors"
            );
            anyhow::bail!(
                "BigQuery insert errors for {}: {} rows",
                table_id,
                errors.len()
            );
        }
        Ok(())
    }

    async fn merge_staging(
        &self,
        target: &str,
        staging: &str,
        schema: &TableSchema,
        primary_key: &str,
    ) -> Result<()> {
        if self.dry_run {
            info!("dry-run: merge staging {} into {}", staging, target);
            return Ok(());
        }

        let schema = with_metadata_schema(schema, &self.metadata);
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let updates: Vec<String> = columns
            .iter()
            .map(|col| format!("{} = S.{}", bq_ident(col), bq_ident(col)))
            .collect();
        let insert_cols: Vec<String> = columns.iter().map(|col| bq_ident(col)).collect();
        let insert_vals: Vec<String> = columns
            .iter()
            .map(|col| format!("S.{}", bq_ident(col)))
            .collect();

        let sql = format!(
            "MERGE `{project}.{dataset}.{target}` T USING `{project}.{dataset}.{staging}` S ON T.{pk} = S.{pk} \
             WHEN MATCHED THEN UPDATE SET {updates} \
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
            project = self.config.project_id,
            dataset = self.config.dataset,
            target = target,
            staging = staging,
            pk = bq_ident(primary_key),
            updates = updates.join(", "),
            insert_cols = insert_cols.join(", "),
            insert_vals = insert_vals.join(", ")
        );

        self.run_query(&sql).await.with_context(|| {
            format!("merging staging BigQuery table {} into {}", staging, target)
        })?;
        Ok(())
    }

    async fn append_rows_via_batch_load(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
    ) -> Result<bool> {
        if !self.batch_load_enabled() {
            return Ok(false);
        }

        let bucket = match &self.config.batch_load_bucket {
            Some(bucket) => bucket,
            None => return Ok(false),
        };
        let token_source = match &self.gcs_token_source {
            Some(token_source) => token_source,
            None => anyhow::bail!("GCS batch load requested but token source is unavailable"),
        };

        let schema = with_metadata_schema(schema, &self.metadata);
        let format = if parquet_batch_load_supported(&schema) {
            BatchLoadFormat::Parquet
        } else {
            warn!(
                table = %table_id,
                "parquet batch load unsupported for schema; falling back to ndjson"
            );
            BatchLoadFormat::Ndjson
        };

        let object_name = batch_load_object_name(
            self.config.batch_load_prefix.as_deref(),
            table_id,
            format.file_extension(),
        );
        let object_uri = format!("gs://{}/{}", bucket, object_name);
        let body = match format {
            BatchLoadFormat::Ndjson => dataframe_to_ndjson_bytes(frame)?,
            BatchLoadFormat::Parquet => dataframe_to_parquet_bytes(frame, &schema)?,
        };
        self.upload_batch_load_object(
            token_source,
            bucket,
            &object_name,
            format.content_type(),
            body,
        )
        .await
        .with_context(|| format!("uploading batch load object {}", object_uri))?;

        self.run_load_job(table_id, &schema, &object_uri, format)
            .await?;
        Ok(true)
    }

    async fn upload_batch_load_object(
        &self,
        token_source: &Arc<dyn TokenSource>,
        bucket: &str,
        object_name: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<()> {
        let token = token_source
            .token()
            .await
            .map_err(|err| anyhow::anyhow!("fetching GCS access token failed: {}", err))?;
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            bucket,
            urlencoding::encode(object_name)
        );
        let response = reqwest::Client::new()
            .post(url)
            .header("Authorization", token)
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("GCS upload failed: {} {}", status, body);
        }
        Ok(())
    }

    async fn run_load_job(
        &self,
        table_id: &str,
        schema: &TableSchema,
        source_uri: &str,
        format: BatchLoadFormat,
    ) -> Result<()> {
        let job_id = format!("cdsync_load_{}", Uuid::new_v4().simple());
        let location = self.config.location.clone();
        let job = Job {
            job_reference: JobReference {
                project_id: self.config.project_id.clone(),
                job_id: job_id.clone(),
                location: location.clone(),
            },
            configuration: JobConfiguration {
                job_type: "LOAD".to_string(),
                job: JobType::Load(JobConfigurationLoad {
                    source_uris: vec![source_uri.to_string()],
                    schema: Some(BqTableSchema {
                        fields: bq_fields_from_schema(&schema.columns),
                    }),
                    destination_table: TableReference {
                        project_id: self.config.project_id.clone(),
                        dataset_id: self.config.dataset.clone(),
                        table_id: table_id.to_string(),
                    },
                    create_disposition: Some(CreateDisposition::CreateIfNeeded),
                    write_disposition: Some(WriteDisposition::WriteAppend),
                    source_format: Some(format.source_format()),
                    max_bad_records: Some(0),
                    autodetect: Some(false),
                    ignore_unknown_values: Some(false),
                    parquet_options: match format {
                        BatchLoadFormat::Parquet => Some(ParquetOptions::default()),
                        BatchLoadFormat::Ndjson => None,
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let created = self
            .client
            .job()
            .create(&job)
            .await
            .with_context(|| format!("creating BigQuery load job for {}", source_uri))?;

        self.wait_for_job_completion(&created).await
    }

    async fn wait_for_job_completion(&self, job: &Job) -> Result<()> {
        let job_id = &job.job_reference.job_id;
        let location = job.job_reference.location.clone();
        loop {
            let current = self
                .client
                .job()
                .get(
                    &self.config.project_id,
                    job_id,
                    &GetJobRequest {
                        location: location.clone(),
                    },
                )
                .await
                .with_context(|| format!("fetching BigQuery job {}", job_id))?;

            if current.status.state == JobState::Done {
                if let Some(error_result) = current.status.error_result {
                    anyhow::bail!("BigQuery load job {} failed: {:?}", job_id, error_result);
                }
                if let Some(errors) = current.status.errors
                    && !errors.is_empty()
                {
                    anyhow::bail!("BigQuery load job {} reported errors: {:?}", job_id, errors);
                }
                return Ok(());
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[async_trait]
impl Destination for BigQueryDestination {
    async fn ensure_table(&self, schema: &TableSchema) -> Result<()> {
        self.ensure_table_internal(schema, &schema.name, true).await
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        if self.dry_run {
            info!("dry-run: truncate {}", table);
            return Ok(());
        }
        self.invalidate_storage_writer(table).await;
        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}",
                emulator_http, self.config.project_id, self.config.dataset, table
            );
            let response = client.delete(url).send().await?;
            if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            error!(
                table = %table,
                status = %response.status(),
                "BigQuery emulator delete table failed"
            );
            anyhow::bail!("emulator delete table failed: {}", response.status());
        }
        let sql = format!(
            "TRUNCATE TABLE `{project}.{dataset}.{table}`",
            project = self.config.project_id,
            dataset = self.config.dataset,
            table = table
        );
        self.run_query(&sql)
            .await
            .with_context(|| format!("truncating BigQuery table {}", table))?;
        Ok(())
    }

    async fn write_batch(
        &self,
        table: &str,
        schema: &TableSchema,
        frame: &DataFrame,
        mode: WriteMode,
        primary_key: Option<&str>,
    ) -> Result<()> {
        match mode {
            WriteMode::Append => {
                self.append_rows(table, schema, frame, primary_key).await?;
            }
            WriteMode::Upsert => {
                if let Some(pk) = primary_key {
                    if self.config.emulator_http.is_some() {
                        warn!(
                            table,
                            "bigquery emulator: upsert unsupported; falling back to append"
                        );
                        self.append_rows(table, schema, frame, Some(pk)).await?;
                        return Ok(());
                    }
                    let staging = format!("{table}_staging");
                    self.ensure_table_internal(schema, &staging, false).await?;
                    self.truncate_table(&staging).await?;
                    self.append_rows(&staging, schema, frame, Some(pk)).await?;
                    self.merge_staging(table, &staging, schema, pk).await?;
                } else {
                    warn!("no primary key for {table}; falling back to append");
                    self.append_rows(table, schema, frame, None).await?;
                }
            }
        }
        Ok(())
    }
}

fn bq_fields_from_schema(columns: &[ColumnSchema]) -> Vec<TableFieldSchema> {
    columns
        .iter()
        .map(|col| {
            let data_type = match col.data_type {
                DataType::String => TableFieldType::String,
                DataType::Int64 => TableFieldType::Int64,
                DataType::Float64 => TableFieldType::Float64,
                DataType::Bool => TableFieldType::Bool,
                DataType::Timestamp => TableFieldType::Timestamp,
                DataType::Date => TableFieldType::Date,
                DataType::Bytes => TableFieldType::Bytes,
                DataType::Numeric => TableFieldType::Numeric,
                DataType::Json => TableFieldType::Json,
            };
            TableFieldSchema {
                name: col.name.clone(),
                data_type,
                // Replication batches may legitimately contain NULLs for fields that are
                // declared NOT NULL at the source, for example during CDC updates that omit
                // unchanged toasted columns. Keeping destination fields nullable avoids
                // load-job failures while preserving data values and metadata.
                mode: Some(gcloud_bigquery::http::table::TableFieldMode::Nullable),
                ..Default::default()
            }
        })
        .collect()
}

fn bq_ident(name: &str) -> String {
    let escaped = name.replace('`', "\\`");
    format!("`{}`", escaped)
}

fn batch_load_object_name(prefix: Option<&str>, table_id: &str, extension: &str) -> String {
    let base = format!(
        "{}_{}.{}",
        table_id.replace('.', "_"),
        Uuid::new_v4().simple(),
        extension
    );
    match prefix.map(str::trim).filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), base),
        None => base,
    }
}

impl BatchLoadFormat {
    fn file_extension(self) -> &'static str {
        match self {
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Ndjson => "application/x-ndjson",
            Self::Parquet => "application/vnd.apache.parquet",
        }
    }

    fn source_format(self) -> SourceFormat {
        match self {
            Self::Ndjson => SourceFormat::NewlineDelimitedJson,
            Self::Parquet => SourceFormat::Parquet,
        }
    }
}

fn dataframe_to_ndjson_bytes(frame: &DataFrame) -> Result<Vec<u8>> {
    let rows = dataframe_to_json_rows(frame)?;
    let mut out = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut out, &row)?;
        out.push(b'\n');
    }
    Ok(out)
}

fn dataframe_to_parquet_bytes(frame: &DataFrame, schema: &TableSchema) -> Result<Vec<u8>> {
    let mut parquet_frame = parquet_batch_load_frame(frame, schema)?;
    let mut cursor = Cursor::new(Vec::new());
    ParquetWriter::new(&mut cursor)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut parquet_frame)
        .context("writing parquet batch load payload")?;
    Ok(cursor.into_inner())
}

fn parquet_batch_load_supported(schema: &TableSchema) -> bool {
    schema
        .columns
        .iter()
        .all(|column| !matches!(column.data_type, DataType::Numeric | DataType::Json))
}

fn parquet_batch_load_frame(frame: &DataFrame, schema: &TableSchema) -> Result<DataFrame> {
    let mut parquet_frame = frame.clone();
    for column in &schema.columns {
        let idx = parquet_frame
            .try_get_column_index(&column.name)
            .with_context(|| format!("missing batch load column {}", column.name))?;
        let series = parquet_batch_load_series(
            parquet_frame
                .column(&column.name)
                .with_context(|| format!("missing batch load column {}", column.name))?
                .as_materialized_series(),
            column,
        )?;
        parquet_frame
            .replace_column(idx, series)
            .with_context(|| format!("replacing batch load column {}", column.name))?;
    }
    Ok(parquet_frame)
}

fn parquet_batch_load_series(series: &Series, column: &ColumnSchema) -> Result<Series> {
    match column.data_type {
        DataType::String => {
            let values = collect_parquet_string_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Int64 => {
            let values = collect_parquet_int64_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Float64 => {
            let values = collect_parquet_float64_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Bool => {
            let values = collect_parquet_bool_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Timestamp => {
            let values = collect_parquet_timestamp_values(series)?;
            let mut out = Series::new(column.name.as_str().into(), values);
            out = out.cast(&polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Microseconds,
                None,
            ))?;
            Ok(out)
        }
        DataType::Date => {
            let values = collect_parquet_date_values(series)?;
            let mut out = Series::new(column.name.as_str().into(), values);
            out = out.cast(&polars::prelude::DataType::Date)?;
            Ok(out)
        }
        DataType::Bytes => {
            let values = collect_parquet_binary_values(series)?;
            let refs: Vec<Option<&[u8]>> = values.iter().map(|value| value.as_deref()).collect();
            Ok(BinaryChunked::from_slice_options(column.name.as_str().into(), &refs).into_series())
        }
        DataType::Numeric | DataType::Json => anyhow::bail!(
            "parquet batch load does not support {:?} columns yet: {}",
            column.data_type,
            column.name
        ),
    }
}

fn supports_storage_write_schema(schema: &TableSchema) -> bool {
    schema
        .columns
        .iter()
        .all(|column| is_valid_proto_field_name(&column.name))
}

fn is_valid_proto_field_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn storage_write_schema_key(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|column| format!("{}:{:?}:{}", column.name, column.data_type, column.nullable))
        .collect::<Vec<_>>()
        .join("|")
}

fn build_storage_write_descriptor(schema: &TableSchema) -> DescriptorProto {
    DescriptorProto {
        name: Some("CdsyncRow".to_string()),
        field: schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| FieldDescriptorProto {
                name: Some(column.name.clone()),
                number: Some((idx + 1) as i32),
                label: Some(Label::Optional as i32),
                r#type: Some(storage_write_field_type(column.data_type.clone()) as i32),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn storage_write_field_type(data_type: DataType) -> Type {
    match data_type {
        DataType::String | DataType::Date | DataType::Numeric | DataType::Json => Type::String,
        DataType::Int64 => Type::Int64,
        DataType::Float64 => Type::Double,
        DataType::Bool => Type::Bool,
        DataType::Timestamp => Type::Int64,
        DataType::Bytes => Type::Bytes,
    }
}

fn build_message_descriptor(descriptor_proto: &DescriptorProto) -> Result<MessageDescriptor> {
    let file_descriptor = FileDescriptorProto {
        name: Some("cdsync_storage_write.proto".to_string()),
        syntax: Some("proto2".to_string()),
        message_type: vec![descriptor_proto.clone()],
        ..Default::default()
    };
    let file_descriptor_set = FileDescriptorSet {
        file: vec![file_descriptor],
    };
    let descriptor_pool = DescriptorPool::decode(file_descriptor_set.encode_to_vec().as_slice())
        .context("building storage write descriptor pool")?;
    descriptor_pool
        .get_message_by_name("CdsyncRow")
        .context("missing storage write message descriptor")
}

fn encode_storage_write_rows(
    frame: &DataFrame,
    schema: &TableSchema,
    message_descriptor: &MessageDescriptor,
) -> Result<Vec<Vec<u8>>> {
    let columns = frame.get_column_names();
    let height = frame.height();
    let mut rows = Vec::with_capacity(height);
    let mut row = PolarsRow::new(vec![AnyValue::Null; columns.len()]);
    for idx in 0..height {
        frame.get_row_amortized(idx, &mut row)?;
        let mut message = DynamicMessage::new(message_descriptor.clone());
        for (column, value) in schema.columns.iter().zip(row.0.iter()) {
            if let Some(reflect_value) = anyvalue_to_storage_write_value(&column.data_type, value)?
            {
                let field = message_descriptor
                    .get_field_by_name(&column.name)
                    .with_context(|| format!("missing proto field {}", column.name))?;
                message.set_field(&field, reflect_value);
            }
        }
        rows.push(message.encode_to_vec());
    }
    Ok(rows)
}

fn anyvalue_to_storage_write_value(
    data_type: &DataType,
    value: &AnyValue,
) -> Result<Option<ReflectValue>> {
    if matches!(value, AnyValue::Null) {
        return Ok(None);
    }
    let reflect_value = match data_type {
        DataType::String | DataType::Date | DataType::Numeric | DataType::Json => {
            ReflectValue::String(anyvalue_to_owned_string(value)?)
        }
        DataType::Int64 => ReflectValue::I64(anyvalue_to_i64(value)?),
        DataType::Float64 => ReflectValue::F64(anyvalue_to_f64(value)?),
        DataType::Bool => ReflectValue::Bool(anyvalue_to_bool(value)?),
        DataType::Timestamp => ReflectValue::I64(anyvalue_to_timestamp_micros(value)?),
        DataType::Bytes => ReflectValue::Bytes(Bytes::from(anyvalue_to_bytes(value)?)),
    };
    Ok(Some(reflect_value))
}

fn dataframe_to_json_rows(frame: &DataFrame) -> Result<Vec<Map<String, Value>>> {
    let columns = frame.get_column_names();
    let height = frame.height();
    let mut output = Vec::with_capacity(height);
    let mut row = PolarsRow::new(vec![AnyValue::Null; columns.len()]);
    for idx in 0..height {
        frame.get_row_amortized(idx, &mut row)?;
        let mut map = Map::with_capacity(columns.len());
        for (col_name, value) in columns.iter().zip(row.0.iter()) {
            map.insert(col_name.to_string(), anyvalue_to_json(value));
        }
        output.push(map);
    }
    Ok(output)
}

fn collect_parquet_string_values(series: &Series) -> Result<Vec<Option<String>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_owned_string(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_int64_values(series: &Series) -> Result<Vec<Option<i64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_i64(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_float64_values(series: &Series) -> Result<Vec<Option<f64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_f64(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_bool_values(series: &Series) -> Result<Vec<Option<bool>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_bool(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_timestamp_values(series: &Series) -> Result<Vec<Option<i64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_timestamp_micros(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_date_values(series: &Series) -> Result<Vec<Option<i32>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_date_days(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_binary_values(series: &Series) -> Result<Vec<Option<Vec<u8>>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_bytes(&value).map(Some)
            }
        })
        .collect()
}

fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int64(v) => Value::Number((*v).into()),
        AnyValue::Int32(v) => Value::Number((*v as i64).into()),
        AnyValue::UInt64(v) => Value::Number((*v).into()),
        AnyValue::UInt32(v) => Value::Number((*v as u64).into()),
        AnyValue::Float64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::Float32(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::StringOwned(v) => Value::String(v.to_string()),
        AnyValue::Binary(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::BinaryOwned(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::Datetime(ts, unit, _) => Value::String(datetime_to_rfc3339(*ts, *unit)),
        AnyValue::DatetimeOwned(ts, unit, _) => Value::String(datetime_to_rfc3339(*ts, *unit)),
        AnyValue::Date(days) => Value::String(date_to_string(*days)),
        AnyValue::Decimal(v, _) => Value::String(v.to_string()),
        other => Value::String(other.to_string()),
    }
}

fn datetime_to_rfc3339(ts: i64, unit: polars::prelude::TimeUnit) -> String {
    let nanos = match unit {
        polars::prelude::TimeUnit::Nanoseconds => ts,
        polars::prelude::TimeUnit::Microseconds => ts * 1_000,
        polars::prelude::TimeUnit::Milliseconds => ts * 1_000_000,
    };
    let seconds = nanos / 1_000_000_000;
    let nanos_part = (nanos % 1_000_000_000) as u32;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos_part)
        .unwrap_or_else(chrono::Utc::now);
    dt.to_rfc3339()
}

fn date_to_string(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date"));
    let date = epoch + chrono::Duration::days(days as i64);
    date.format("%Y-%m-%d").to_string()
}

fn value_to_insert_id(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        other => serde_json::to_string(other).ok(),
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}

fn tuple_value_as_i64(tuple: &BqTuple, index: usize) -> Result<i64> {
    match tuple.f.get(index).map(|cell| &cell.v) {
        Some(BqValue::String(value)) => value
            .parse::<i64>()
            .with_context(|| format!("parsing bigint cell at index {}", index)),
        Some(BqValue::Null) | None => Ok(0),
        other => anyhow::bail!("unexpected bigint cell at index {}: {:?}", index, other),
    }
}

fn tuple_value_as_datetime(tuple: &BqTuple, index: usize) -> Result<Option<DateTime<Utc>>> {
    match tuple.f.get(index).map(|cell| &cell.v) {
        Some(BqValue::String(value)) if !value.is_empty() => {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
                return Ok(Some(parsed.with_timezone(&Utc)));
            }
            let seconds = value
                .parse::<f64>()
                .with_context(|| format!("parsing datetime cell at index {}", index))?;
            let whole_seconds = seconds.trunc() as i64;
            let nanos =
                ((seconds.fract() * 1_000_000_000.0).round() as i64).clamp(0, 999_999_999) as u32;
            Ok(DateTime::<Utc>::from_timestamp(whole_seconds, nanos))
        }
        Some(BqValue::Null) | None | Some(BqValue::String(_)) => Ok(None),
        other => anyhow::bail!("unexpected datetime cell at index {}: {:?}", index, other),
    }
}

fn anyvalue_to_owned_string(value: &AnyValue) -> Result<String> {
    Ok(match value {
        AnyValue::String(value) => value.to_string(),
        AnyValue::StringOwned(value) => value.to_string(),
        AnyValue::Date(value) => date_to_string(*value),
        AnyValue::Datetime(ts, unit, _) => datetime_to_rfc3339(*ts, *unit),
        AnyValue::DatetimeOwned(ts, unit, _) => datetime_to_rfc3339(*ts, *unit),
        AnyValue::Binary(bytes) => encode_base64(bytes),
        AnyValue::BinaryOwned(bytes) => encode_base64(bytes),
        AnyValue::Boolean(value) => value.to_string(),
        AnyValue::Int64(value) => value.to_string(),
        AnyValue::Int32(value) => value.to_string(),
        AnyValue::UInt64(value) => value.to_string(),
        AnyValue::UInt32(value) => value.to_string(),
        AnyValue::Float64(value) => value.to_string(),
        AnyValue::Float32(value) => value.to_string(),
        AnyValue::Decimal(value, _) => value.to_string(),
        other => other.to_string(),
    })
}

fn anyvalue_to_i64(value: &AnyValue) -> Result<i64> {
    match value {
        AnyValue::Int64(value) => Ok(*value),
        AnyValue::Int32(value) => Ok(*value as i64),
        AnyValue::UInt64(value) => Ok(*value as i64),
        AnyValue::UInt32(value) => Ok(*value as i64),
        AnyValue::String(value) => value
            .parse::<i64>()
            .with_context(|| format!("parsing int64 value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<i64>()
            .with_context(|| format!("parsing int64 value {}", value)),
        other => anyhow::bail!("unsupported int64 value {:?}", other),
    }
}

fn anyvalue_to_f64(value: &AnyValue) -> Result<f64> {
    match value {
        AnyValue::Float64(value) => Ok(*value),
        AnyValue::Float32(value) => Ok(f64::from(*value)),
        AnyValue::Int64(value) => Ok(*value as f64),
        AnyValue::Int32(value) => Ok(*value as f64),
        AnyValue::String(value) => value
            .parse::<f64>()
            .with_context(|| format!("parsing float value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<f64>()
            .with_context(|| format!("parsing float value {}", value)),
        other => anyhow::bail!("unsupported float value {:?}", other),
    }
}

fn anyvalue_to_bool(value: &AnyValue) -> Result<bool> {
    match value {
        AnyValue::Boolean(value) => Ok(*value),
        AnyValue::String(value) => value
            .parse::<bool>()
            .with_context(|| format!("parsing bool value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<bool>()
            .with_context(|| format!("parsing bool value {}", value)),
        other => anyhow::bail!("unsupported bool value {:?}", other),
    }
}

fn anyvalue_to_timestamp_micros(value: &AnyValue) -> Result<i64> {
    match value {
        AnyValue::Int64(value) => Ok(*value),
        AnyValue::String(value) => timestamp_string_to_micros(value),
        AnyValue::StringOwned(value) => timestamp_string_to_micros(&value.to_string()),
        AnyValue::Datetime(ts, unit, _) => Ok(match unit {
            polars::prelude::TimeUnit::Nanoseconds => *ts / 1_000,
            polars::prelude::TimeUnit::Microseconds => *ts,
            polars::prelude::TimeUnit::Milliseconds => *ts * 1_000,
        }),
        AnyValue::DatetimeOwned(ts, unit, _) => Ok(match unit {
            polars::prelude::TimeUnit::Nanoseconds => *ts / 1_000,
            polars::prelude::TimeUnit::Microseconds => *ts,
            polars::prelude::TimeUnit::Milliseconds => *ts * 1_000,
        }),
        other => anyhow::bail!("unsupported timestamp value {:?}", other),
    }
}

fn anyvalue_to_date_days(value: &AnyValue) -> Result<i32> {
    match value {
        AnyValue::Date(value) => Ok(*value),
        AnyValue::String(value) => date_string_to_days(value),
        AnyValue::StringOwned(value) => date_string_to_days(&value.to_string()),
        other => anyhow::bail!("unsupported date value {:?}", other),
    }
}

fn anyvalue_to_bytes(value: &AnyValue) -> Result<Vec<u8>> {
    match value {
        AnyValue::Binary(bytes) => Ok(bytes.to_vec()),
        AnyValue::BinaryOwned(bytes) => Ok(bytes.clone()),
        AnyValue::String(value) => STANDARD
            .decode(value)
            .with_context(|| format!("decoding base64 bytes {}", value)),
        AnyValue::StringOwned(value) => STANDARD
            .decode(value.as_bytes())
            .with_context(|| format!("decoding base64 bytes {}", value)),
        other => anyhow::bail!("unsupported bytes value {:?}", other),
    }
}

fn date_string_to_days(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("parsing date {}", value))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    let days = date.signed_duration_since(epoch).num_days();
    i32::try_from(days).with_context(|| format!("date {} outside supported range", value))
}

fn timestamp_string_to_micros(value: &str) -> Result<i64> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%#z"))
        .with_context(|| format!("parsing timestamp {}", value))?
        .with_timezone(&Utc);
    Ok(timestamp.timestamp_micros())
}

fn validate_storage_write_response(
    table_id: &str,
    response: &AppendRowsResponse,
) -> Result<Option<bool>> {
    if !response.row_errors.is_empty() {
        crate::telemetry::record_bigquery_row_errors(table_id, response.row_errors.len() as u64);
        anyhow::bail!(
            "storage write row errors for {}: {} rows",
            table_id,
            response.row_errors.len()
        );
    }

    match &response.response {
        Some(AppendRowsStreamResponse::AppendResult(_)) | None => Ok(None),
        Some(AppendRowsStreamResponse::Error(status))
            if status.code == Code::AlreadyExists as i32 =>
        {
            Ok(Some(true))
        }
        Some(AppendRowsStreamResponse::Error(status)) => {
            anyhow::bail!(
                "storage write stream error for {}: {}",
                table_id,
                status.message
            )
        }
    }
}

#[cfg(test)]
mod storage_write_tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType, MetadataColumns, TableSchema};
    use chrono::TimeZone;
    use gcloud_googleapis::cloud::bigquery::storage::v1::AppendRowsResponse;
    use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult, Response,
    };
    use gcloud_googleapis::rpc::Status;
    use polars::io::SerReader;
    use polars::io::parquet::read::ParquetReader;
    use polars::prelude::NamedFrom;
    use prost_reflect::Value as ReflectValue;
    use std::borrow::Cow;
    use std::io::Cursor;

    fn schema() -> TableSchema {
        TableSchema {
            name: "public__items".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "updated_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    data_type: DataType::Json,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        }
    }

    fn parquet_schema() -> TableSchema {
        TableSchema {
            name: "public__events".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "occurred_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                },
                ColumnSchema {
                    name: "event_date".to_string(),
                    data_type: DataType::Date,
                    nullable: false,
                },
                ColumnSchema {
                    name: "raw_bytes".to_string(),
                    data_type: DataType::Bytes,
                    nullable: true,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        }
    }

    #[test]
    fn storage_write_schema_supports_valid_names() {
        assert!(supports_storage_write_schema(&schema()));
        assert!(!supports_storage_write_schema(&TableSchema {
            name: "bad".to_string(),
            columns: vec![ColumnSchema {
                name: "bad-name".to_string(),
                data_type: DataType::String,
                nullable: true,
            }],
            primary_key: None,
        }));
    }

    #[test]
    fn bigquery_fields_are_always_nullable_for_replication() {
        let fields = bq_fields_from_schema(&schema().columns);
        assert!(fields.iter().all(|field| {
            field.mode == Some(gcloud_bigquery::http::table::TableFieldMode::Nullable)
        }));
    }

    #[test]
    fn storage_write_descriptor_round_trip_encodes_rows() {
        let schema = schema();
        let descriptor = build_storage_write_descriptor(&schema);
        let message_descriptor =
            build_message_descriptor(&descriptor).expect("message descriptor should build");

        let updated_at = Utc.with_ymd_and_hms(2026, 3, 26, 12, 0, 0).unwrap();
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1i64]).into(),
            polars::prelude::Series::new("updated_at".into(), &[updated_at.to_rfc3339()]).into(),
            polars::prelude::Series::new("payload".into(), &[r#"{"k":"v"}"#]).into(),
        ])
        .expect("frame");

        let rows =
            encode_storage_write_rows(&frame, &schema, &message_descriptor).expect("encoded rows");
        assert_eq!(rows.len(), 1);

        let message = DynamicMessage::decode(message_descriptor, rows[0].as_slice()).unwrap();
        let id = message
            .get_field_by_name("id")
            .expect("id field should exist");
        assert_eq!(id, Cow::Owned(ReflectValue::I64(1)));
    }

    #[test]
    fn storage_write_response_errors_fail() {
        let response = AppendRowsResponse {
            response: Some(Response::Error(Status {
                code: Code::Internal as i32,
                message: "boom".to_string(),
                details: Vec::new(),
            })),
            ..Default::default()
        };

        let err = validate_storage_write_response("items", &response).expect_err("should fail");
        assert!(err.to_string().contains("storage write stream error"));
    }

    #[test]
    fn storage_write_already_exists_response_is_idempotent() {
        let response = AppendRowsResponse {
            response: Some(Response::Error(Status {
                code: Code::AlreadyExists as i32,
                message: "duplicate".to_string(),
                details: Vec::new(),
            })),
            ..Default::default()
        };

        assert_eq!(
            validate_storage_write_response("items", &response).expect("should be handled"),
            Some(true)
        );

        let success = AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset: Some(1) })),
            ..Default::default()
        };
        assert_eq!(
            validate_storage_write_response("items", &success).expect("append result"),
            None
        );
    }

    #[test]
    fn timestamp_string_to_micros_accepts_postgres_style_timestamptz() {
        let micros = timestamp_string_to_micros("2026-03-30 01:36:12.186373+00")
            .expect("postgres timestamptz");
        let dt = DateTime::<Utc>::from_timestamp_micros(micros).expect("valid micros");
        assert_eq!(dt.to_rfc3339(), "2026-03-30T01:36:12.186373+00:00");
    }

    #[test]
    fn batch_load_object_name_uses_prefix_when_present() {
        let name = batch_load_object_name(Some("staging/app"), "public__items", "parquet");
        assert!(name.starts_with("staging/app/public__items_"));
        assert!(name.ends_with(".parquet"));
    }

    #[test]
    fn parquet_batch_load_supported_rejects_json_and_numeric() {
        assert!(!parquet_batch_load_supported(&schema()));
        let numeric_schema = TableSchema {
            name: "public__prices".to_string(),
            columns: vec![ColumnSchema {
                name: "amount".to_string(),
                data_type: DataType::Numeric,
                nullable: true,
            }],
            primary_key: None,
        };
        assert!(!parquet_batch_load_supported(&numeric_schema));
        assert!(parquet_batch_load_supported(&parquet_schema()));
    }

    #[test]
    fn dataframe_to_parquet_bytes_round_trips_supported_batch_load_types() {
        let metadata = MetadataColumns::default();
        let schema = parquet_schema();
        let full_schema = with_metadata_schema(&schema, &metadata);
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1_i64]).into(),
            polars::prelude::Series::new(
                "occurred_at".into(),
                &["2026-03-30T01:36:12.186373+00:00"],
            )
            .into(),
            polars::prelude::Series::new("event_date".into(), &["2026-03-30"]).into(),
            polars::prelude::Series::new("raw_bytes".into(), &[STANDARD.encode(b"abc")]).into(),
            polars::prelude::Series::new("name".into(), &["alpha"]).into(),
            polars::prelude::Series::new(
                metadata.synced_at.as_str().into(),
                &["2026-03-30T01:36:12.186373+00:00"],
            )
            .into(),
            polars::prelude::Series::new(
                metadata.deleted_at.as_str().into(),
                &[Option::<&str>::None],
            )
            .into(),
        ])
        .expect("frame");

        let payload = dataframe_to_parquet_bytes(&frame, &full_schema).expect("parquet bytes");
        let parquet = ParquetReader::new(Cursor::new(payload))
            .finish()
            .expect("read parquet");

        assert_eq!(
            parquet.column("id").expect("id").dtype(),
            &polars::prelude::DataType::Int64
        );
        assert!(matches!(
            parquet.column("occurred_at").expect("occurred_at").dtype(),
            polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Microseconds, _)
        ));
        assert_eq!(
            parquet.column("event_date").expect("event_date").dtype(),
            &polars::prelude::DataType::Date
        );
        assert_eq!(
            parquet.column("raw_bytes").expect("raw_bytes").dtype(),
            &polars::prelude::DataType::Binary
        );
        assert_eq!(
            parquet.column("name").expect("name").dtype(),
            &polars::prelude::DataType::String
        );
        assert!(matches!(
            parquet
                .column(metadata.synced_at.as_str())
                .expect("synced_at")
                .dtype(),
            polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Microseconds, _)
        ));
        assert_eq!(
            parquet
                .column(metadata.deleted_at.as_str())
                .expect("deleted_at")
                .null_count(),
            1
        );
    }

    #[test]
    fn dataframe_to_ndjson_bytes_emits_one_json_object_per_line() {
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1_i64, 2_i64]).into(),
            polars::prelude::Series::new("name".into(), &["alpha", "beta"]).into(),
        ])
        .expect("frame");

        let payload = dataframe_to_ndjson_bytes(&frame).expect("ndjson bytes");
        let text = String::from_utf8(payload).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[0].contains("\"name\":\"alpha\""));
        assert!(lines[1].contains("\"id\":2"));
        assert!(lines[1].contains("\"name\":\"beta\""));
    }
}
