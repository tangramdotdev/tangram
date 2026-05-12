use {crate::Cli, tangram_client::prelude::*};

/// List groups.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_groups(tg::group::list::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to list the groups"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
