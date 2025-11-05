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
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
