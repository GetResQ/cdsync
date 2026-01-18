use crate::types::TableCheckpoint;
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncState {
    #[serde(default)]
    pub connections: HashMap<String, ConnectionState>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PostgresCdcState {
    pub last_lsn: Option<String>,
    pub slot_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionState {
    #[serde(default)]
    pub postgres: HashMap<String, TableCheckpoint>,
    pub postgres_cdc: Option<PostgresCdcState>,
    #[serde(default)]
    pub salesforce: HashMap<String, TableCheckpoint>,
    pub last_sync_started_at: Option<DateTime<Utc>>,
    pub last_sync_finished_at: Option<DateTime<Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error: Option<String>,
}

impl SyncState {
    pub async fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(SyncState::default());
        }
        let contents = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("reading state file {}", path.display()))?;
        let state = serde_json::from_str(&contents)?;
        Ok(state)
    }

    pub async fn save(&mut self, path: &Path) -> anyhow::Result<()> {
        self.updated_at = Some(Utc::now());
        let contents = serde_json::to_string_pretty(self)?;
        tokio::fs::write(path, contents)
            .await
            .with_context(|| format!("writing state file {}", path.display()))?;
        Ok(())
    }
}
