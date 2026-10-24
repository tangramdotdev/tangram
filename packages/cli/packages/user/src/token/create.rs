use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Create a user token.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::token::create::Arg {
		location: args.location.get(),
	};
	let output = client
		.create_user_token(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the user token"))?;
	cli.print_serde(output.data, args.print).await?;

	Ok(())
}
