use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Add a group member.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub group: tg::Referent<tg::group::Selector>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	pub member: tg::group::Member,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::group::members::add::Arg {
		location: args.location.get_for_options(&args.group),
		member: args.member.clone(),
		tokens: args.group.options.tokens,
	};
	client
		.add_group_member(&args.group.node, arg)
		.await
		.map_err(|error| tg::error!(!error, group = %args.group.node, member = %args.member, "failed to add the group member"))?;
	Ok(())
}
