use {crate::Cli, tangram_client::prelude::*};

/// List groups.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::list::Arg {
			location: args.location.get(),
		};
		let output = client
			.list_groups(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the groups"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
