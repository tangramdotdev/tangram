use crate::Cli;
use tangram_error::{error, Result};

/// Upgrade to the latest version of tangram.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_upgrade(&self, _args: Args) -> Result<()> {
		tokio::process::Command::new("/bin/sh")
			.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"])
			.status()
			.await
			.map_err(|error| error!(source = error, "failed to run the installer"))?;
		Ok(())
	}
}
