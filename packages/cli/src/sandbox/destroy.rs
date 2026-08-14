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
		let sandbox = tg::Sandbox::with_id(args.sandbox);
		let id = sandbox.id().clone();
		sandbox
			.destroy_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, sandbox = %id, "failed to destroy the sandbox"))?;
		Ok(())
	}
}
