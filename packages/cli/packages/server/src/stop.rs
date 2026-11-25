use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Stop the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Whether to send SIGKILL to the server if it does not exit after SIGTERM.
	#[arg(
		default_missing_value = "true",
		id = "server.stop.force",
		long = "force",
		num_args = 0..=1,
		overrides_with = "server.stop.no_force",
		require_equals = true,
	)]
	force: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "server.stop.no_force",
		long = "no-force",
		num_args = 0..=1,
		overrides_with = "server.stop.force",
		require_equals = true,
	)]
	no_force: Option<bool>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let force = args
		.force
		.or(args.no_force.map(|value| !value))
		.unwrap_or(false);
	if force {
		cli.force_stop_server().await?;
	} else {
		cli.stop_server().await?;
	}
	Ok(())
}
