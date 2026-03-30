use anyhow::Result;
use reqwest::Client;

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
