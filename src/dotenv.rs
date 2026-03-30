use anyhow::{Result, anyhow};
use std::sync::OnceLock;

static DOTENV_LOAD_RESULT: OnceLock<Result<(), String>> = OnceLock::new();

pub fn load_dotenv() -> Result<()> {
    let result = DOTENV_LOAD_RESULT.get_or_init(|| match dotenvy::dotenv() {
        Ok(_) => Ok(()),
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    });

    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(anyhow!("loading .env failed: {error}")),
    }
}
