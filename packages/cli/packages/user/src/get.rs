use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[command(flatten)]
	pub ttl: tangram_cli_get::Ttl,

	#[arg(index = 1)]
	pub user: tg::Referent<tg::user::Selector>,
}

pub async fn command_user_get(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::get::Arg {
		cached: args.cached,
		location: args.location.get_for_options(&args.user),
		tokens: args.user.options.tokens,
		ttl: args.ttl.get(),
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
