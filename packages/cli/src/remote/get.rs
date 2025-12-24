use {crate::Cli, tangram_client::prelude::*};

/// Get a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_remote_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle
			.try_get_remote(&args.name)
			.await
			.map_err(|source| tg::error!(!source, name = %args.name, "failed to get the remote"))?
			.ok_or_else(|| tg::error!(name = %args.name, "failed to find the remote"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
