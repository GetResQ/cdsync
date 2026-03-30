use anyhow::Result;

pub fn load_dotenv() -> Result<()> {
    cdsync::dotenv::load_dotenv()
}
