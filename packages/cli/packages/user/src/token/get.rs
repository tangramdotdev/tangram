use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a user token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub token: tg::Referent<tg::token::Id>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::token::get::Arg {
		location: args.location.get_for_options(&args.token),
	};
	let token = args.token.node;
	let output = client
		.try_get_user_token(&token, arg)
		.await
		.map_err(|error| tg::error!(!error, %token, "failed to get the user token"))?
		.ok_or_else(|| tg::error!(%token, "failed to find the user token"))?;
	let tg::user::token::get::Output { data, location } = output;
	cli.print_location_and_tokens(location.as_ref(), &tg::authorization::Tokens::default())?;
	cli.print_serde(data, args.print).await?;

	Ok(())
}
