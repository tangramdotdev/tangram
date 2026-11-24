use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete an organization.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub organization: tg::Referent<tg::organization::Selector>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::organization::delete::Arg {
		location: args.location.get_for_options(&args.organization),
		tokens: args.organization.options.tokens,
	};
	client.delete_organization(&args.organization.node, arg).await.map_err(
		|error| tg::error!(!error, organization = %args.organization.node, "failed to delete the organization"),
	)?;
	Ok(())
}
