use cdsync::config::StatsConfig;
use cdsync::stats::{StatsDb, StatsHandle};
use uuid::Uuid;

fn test_stats_config() -> Option<(String, StatsConfig)> {
    let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
    let config = StatsConfig {
        url: Some(url.clone()),
        schema: Some(format!("cdsync_stats_it_{}", Uuid::new_v4().simple())),
    };
    Some((url, config))
}

#[tokio::test]
async fn postgres_stats_db_migrate_creates_schema_and_tables() -> anyhow::Result<()> {
    let Some((default_url, config)) = test_stats_config() else {
        return Ok(());
    };

    StatsDb::migrate_with_config(&config, &default_url).await?;
    let db = StatsDb::new(&config, &default_url).await?;
    let stats = StatsHandle::new("app");
    db.persist_run(&stats).await?;

    let runs = db.recent_runs(Some("app"), 5).await?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status.as_deref(), Some("running"));

    stats.finish("success", None).await;
    db.persist_run(&stats).await?;

    let runs = db.recent_runs(Some("app"), 5).await?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status.as_deref(), Some("success"));

    Ok(())
}
