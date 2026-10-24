use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Log out.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,
}

pub async fn command_user_logout(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let location = args.location.get();
	let arg = tg::user::current::Arg {
		location: location.clone(),
	};
	let Some(user) = client.get_current_user(arg).await? else {
		return Ok(());
	};
	let location = location.or_else(|| user.location.map(Into::into));
	let local = location
		.as_ref()
		.and_then(tg::location::Arg::to_location)
		.is_some_and(|location| location.is_local());
	let arg = tg::user::logout::Arg { location };
	client
		.logout(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to log out"))?;
	if local {
		cli.delete_token()?;
	}
	Ok(())
}
