use {crate::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub arg: super::Options,
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = args.arg.into_arg();
		let output = handle
			.create_sandbox(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		Self::print_id(&output.id);
		Ok(())
	}
}
