use {crate::Cli, tangram_client::prelude::*};

/// Delete a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: tg::group::Selector,
}

impl Cli {
	pub async fn command_group_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.delete_group(&args.group).await.map_err(
			|error| tg::error!(!error, group = %args.group, "failed to delete the group"),
		)?;
		Ok(())
	}
}
