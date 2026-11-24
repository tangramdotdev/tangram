use tangram_client::prelude::*;

/// Update Tangram to the latest version.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {}

pub async fn run(_args: Args) -> tg::Result<()> {
	tokio::process::Command::new("/bin/sh")
		.args(["-c", "curl -sSL https://www.tangram.dev/install.sh | sh"])
		.status()
		.await
		.map_err(|error| tg::error!(!error, "failed to run the installer"))?;
	Ok(())
}
