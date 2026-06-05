use {crate::Cli, tangram_client::prelude::*};

/// Remove a group member.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: tg::group::Selector,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 2)]
	pub member: tg::group::Member,
}

impl Cli {
	pub async fn command_group_members_remove(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::members::remove::Arg {
			location: args.location.get(),
		};
		client
			.remove_group_member(&args.group, &args.member, arg)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, member = %args.member, "failed to remove the group member"))?
			.ok_or_else(|| tg::error!("failed to find the group member"))?;
		Ok(())
	}
}
