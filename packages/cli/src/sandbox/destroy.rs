use {crate::Cli, tangram_client::prelude::*};

/// Destroy a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,
}

impl Cli {
	pub async fn command_sandbox_destroy(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::sandbox::destroy::Arg::default();
		client.destroy_sandbox(&args.sandbox, arg).await.map_err(
			|error| tg::error!(!error, sandbox = %args.sandbox, "failed to destroy the sandbox"),
		)?;
		Ok(())
	}
}
