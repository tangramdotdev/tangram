use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Remove a group member.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub group: tg::Referent<tg::group::Selector>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub member: tg::group::Member,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::group::members::remove::Arg {
		location: args.location.get_for_options(&args.group),
		tokens: args.group.options.tokens,
	};
	client
		.remove_group_member(&args.group.node, &args.member, arg)
		.await
		.map_err(|error| tg::error!(!error, group = %args.group.node, member = %args.member, "failed to remove the group member"))?
		.ok_or_else(|| tg::error!("failed to find the group member"))?;
	Ok(())
}
