use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub grant: tg::Referent<tg::grant::Id>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::grant::delete::Arg {
		location: args.location.get_for_options(&args.grant),
	};
	let grant = args.grant.node;
	client
		.try_delete_grant(&grant, arg)
		.await
		.map_err(|error| tg::error!(!error, %grant, "failed to delete the grant"))?
		.ok_or_else(|| tg::error!(%grant, "failed to find the grant"))?;

	Ok(())
}
