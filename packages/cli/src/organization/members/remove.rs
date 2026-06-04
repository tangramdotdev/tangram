use {crate::Cli, tangram_client::prelude::*};

/// Remove an organization member.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub organization: tg::organization::Selector,

	#[arg(index = 2)]
	pub member: tg::organization::Member,
}

impl Cli {
	pub async fn command_organization_members_remove(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.remove_organization_member(&args.organization, &args.member)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization, member = %args.member, "failed to remove the organization member"),
			)?
			.ok_or_else(|| tg::error!("failed to find the organization member"))?;
		Ok(())
	}
}
