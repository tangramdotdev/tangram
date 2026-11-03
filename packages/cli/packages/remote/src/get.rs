use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub principal: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// The remote's ID or name.
	#[arg(index = 1)]
	pub remote: tg::remote::Selector,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::remote::get::Arg {
		principal: args.principal,
	};
	let output = client
		.try_get_remote(&args.remote, arg)
		.await
		.map_err(|error| tg::error!(!error, remote = %args.remote, "failed to get the remote"))?
		.ok_or_else(|| tg::error!(remote = %args.remote, "failed to find the remote"))?;
	cli.print_serde(output.data, args.print).await?;
	Ok(())
}
