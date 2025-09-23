use {crate::Cli, tangram_client as tg};

/// Update Tangram to the latest version.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_tangram_update(&mut self, _args: Args) -> tg::Result<()> {
		tokio::process::Command::new("/bin/sh")
			.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"])
			.status()
			.await
			.map_err(|source| tg::error!(!source, "failed to run the installer"))?;
		Ok(())
	}
}
