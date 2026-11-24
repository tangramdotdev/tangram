use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Add an organization member.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub organization: tg::Referent<tg::organization::Selector>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub member: tg::organization::Member,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
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
