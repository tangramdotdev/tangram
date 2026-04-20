use {crate::Cli, tangram_client::prelude::*};

/// Get a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_sandbox_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle
			.try_get_sandbox(
				&args.sandbox,
				tg::sandbox::get::Arg {
					location: args.locations.get(),
				},
			)
			.await
			.map_err(
				|source| tg::error!(!source, sandbox = %args.sandbox, "failed to get the sandbox"),
			)?
			.ok_or_else(|| tg::error!(sandbox = %args.sandbox, "failed to find the sandbox"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
