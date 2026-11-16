use tangram_client::prelude::*;

/// Update Tangram to the latest version.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

pub async fn run(_args: Args) -> tg::Result<()> {
	let mut command = tokio::process::Command::new("/bin/sh");
	command.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"]);
	tangram_util::spawn::status_async(&mut command)
		.await
		.map_err(|error| tg::error!(!error, "failed to run the installer"))?;
	Ok(())
}
