use {crate::Cli, tangram_client::prelude::*};

/// Create a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub specifier: tg::Specifier,
}

impl Cli {
	pub async fn command_group_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::create::Arg {
			specifier: args.specifier.clone(),
		};
		let output = client.create_group(arg).await.map_err(
			|error| tg::error!(!error, specifier = %args.specifier, "failed to create the group"),
		)?;
		self.print_serde(output.group, args.print).await?;
		Ok(())
	}
}
