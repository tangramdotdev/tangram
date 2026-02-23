use {crate::Cli, tangram_client::prelude::*};

/// Delete a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,
}

impl Cli {
	pub async fn command_sandbox_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let client = handle
			.left()
			.ok_or_else(|| tg::error!("this command requires a client, not a server"))?;

		// Delete the sandbox.
		client
			.delete_sandbox(&args.sandbox)
			.await
			.map_err(|source| {
				tg::error!(!source, id = %args.sandbox, "failed to delete the sandbox")
			})?;

		Ok(())
	}
}
