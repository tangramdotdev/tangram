use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a user.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[usage(long)]
	pub cached: bool,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(flatten)]
	pub time_to_live: tangram_cli_reference::TimeToLive,

	pub user: tg::Referent<tg::user::Selector>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::get::Arg {
		cached: args.cached,
		location: args.location.get_for_options(&args.user),
		time_to_live: args.time_to_live.get(),
		tokens: args.user.options.tokens,
	};
	let user = client
		.try_get_user(&args.user.node, arg)
		.await
		.map_err(|error| tg::error!(!error, user = %args.user.node, "failed to get the user"))?
		.ok_or_else(|| tg::error!(user = %args.user.node, "failed to find the user"))?;
	let tg::user::get::Output {
		data,
		location,
		tokens,
	} = user;
	cli.print_location_and_tokens(location.as_ref(), &tokens)?;
	cli.print_serde(data, args.print).await?;
	Ok(())
}
