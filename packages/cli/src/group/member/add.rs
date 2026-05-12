use {crate::Cli, tangram_client::prelude::*};

/// Add a group member.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: String,

	#[arg(index = 2)]
	pub user: String,
}

impl Cli {
	pub async fn command_group_member_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.add_group_member(&args.group, &args.user)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, user = %args.user, "failed to add the group member"))?;
		Ok(())
	}
}
