use {crate::Cli, tangram_client::prelude::*};

/// Get a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,

	#[arg(long)]
	pub principal: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_remote_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::remote::get::Arg {
			principal: args.principal,
		};
		let output = client
			.try_get_remote(&args.name, arg)
			.await
			.map_err(|error| tg::error!(!error, name = %args.name, "failed to get the remote"))?
			.ok_or_else(|| tg::error!(name = %args.name, "failed to find the remote"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
