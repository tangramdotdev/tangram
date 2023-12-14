use crate::Cli;
use tangram_error::{Result, WrapErr};

/// Upgrade to the latest version of tangram.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_upgrade(&self, _args: Args) -> Result<()> {
		tokio::process::Command::new("/bin/sh")
			.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"])
			.status()
			.await
			.wrap_err("Failed to run the installer.")?;
		Ok(())
	}
}
