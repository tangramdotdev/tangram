use {crate::Cli, tangram_client::prelude::*};

/// List sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub owner: crate::sandbox::Owner,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_sandbox_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let owner = self.resolve_owner(&client, &args.owner).await?;
		let arg = tg::sandbox::list::Arg {
			location: args.locations.get(),
			owner,
		};
		let output = client
			.list_sandboxes(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
