use {crate::Cli, tangram_client::prelude::*};

/// Add a group member.
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
	pub async fn command_group_members_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::group::members::add::Arg {
			location: args.location.get(),
			member: args.member.clone(),
		};
		client
			.add_group_member(&args.group, arg)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, member = %args.member, "failed to add the group member"))?;
		Ok(())
	}
}
