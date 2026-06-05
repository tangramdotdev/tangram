use {crate::Cli, tangram_client::prelude::*};

/// Delete a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub group: tg::group::Selector,
}

impl Cli {
	pub async fn command_group_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::delete::Arg {
			location: args.location.get(),
		};
		client.delete_group(&args.group, arg).await.map_err(
			|error| tg::error!(!error, group = %args.group, "failed to delete the group"),
		)?;
		Ok(())
	}
}
