use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get the current user.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
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
