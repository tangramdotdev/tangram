use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get a user token.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

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
