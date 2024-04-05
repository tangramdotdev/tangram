use crate::Cli;
use tangram_client as tg;

/// Upgrade to the latest version of Tangram.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_upgrade(&self, _args: Args) -> tg::Result<()> {
		tokio::process::Command::new("/bin/sh")
			.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"])
			.status()
			.await
			.map_err(|source| tg::error!(!source, "failed to run the installer"))?;
		Ok(())
	}
}
