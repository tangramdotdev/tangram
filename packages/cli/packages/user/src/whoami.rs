use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the current user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn command_user_whoami(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let location = args.location.to_location()?;
	let client = cli.client().await?;
	let arg = tg::user::current::Arg {
		location: location.map(Into::into),
	};
	let user = client
		.get_current_user(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the user"))?
		.ok_or_else(|| tg::error!("not logged in"))?;
	cli.print_serde(user.data, args.print).await?;
	Ok(())
}
