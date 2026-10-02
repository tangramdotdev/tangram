use {crate::Cli, tangram_client::prelude::*};

/// Add an organization member.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub organization: tg::Referent<tg::organization::Selector>,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 2)]
	pub member: tg::organization::Member,
}

impl Cli {
	pub async fn command_organization_members_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::organization::members::add::Arg {
			location: args.location.get_for_options(&args.organization),
			member: args.member.clone(),
			tokens: args.organization.options.tokens,
		};
		client
			.add_organization_member(&args.organization.node, arg)
			.await
			.map_err(
				|error| tg::error!(!error, organization = %args.organization.node, member = %args.member, "failed to add the organization member"),
			)?;
		Ok(())
	}
}
