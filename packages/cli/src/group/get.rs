use {crate::Cli, tangram_client::prelude::*};

/// Get a group.
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
	pub async fn command_group_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::get::Arg {
			location: args.location.get(),
		};
		let group = client
			.try_get_group(&args.group, arg)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, "failed to get the group"))?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		self.print_serde(group, args.print).await?;
		Ok(())
	}
}
