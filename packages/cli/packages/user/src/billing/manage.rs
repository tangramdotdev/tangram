use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Manage the current user's billing payment method.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::user::billing::manage::Arg {
		location: args.location.get(),
	};
	let output = client
		.manage_user_billing(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to manage the user billing"))?;
	cli.open_url(&output.url);

	Ok(())
}
