use {crate::Cli, tangram_client::prelude::*};

/// List grants for a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: tg::group::Selector,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::grants::Arg {
			location: args.location.get(),
		};
		let output = client
			.try_get_group_grants(&args.group, arg)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, "failed to list the grants"))?
			.ok_or_else(|| tg::error!(group = %args.group, "failed to find the group"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
