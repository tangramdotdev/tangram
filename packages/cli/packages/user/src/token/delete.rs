use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a user token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub token: tg::token::Id,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::token::delete::Arg {
		location: args.location.get(),
	};
	client
		.try_delete_user_token(&args.token, arg)
		.await
		.map_err(
			|error| tg::error!(!error, token = %args.token, "failed to delete the user token"),
		)?
		.ok_or_else(|| tg::error!(token = %args.token, "failed to find the user token"))?;

	Ok(())
}
