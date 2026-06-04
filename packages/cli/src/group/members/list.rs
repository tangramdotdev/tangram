use {crate::Cli, tangram_client::prelude::*};

/// List group members.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: tg::group::Selector,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_members_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client.list_group_members(&args.group).await.map_err(
			|error| tg::error!(!error, group = %args.group, "failed to list the group members"),
		)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
