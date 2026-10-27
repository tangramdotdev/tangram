use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub group: tg::Referent<tg::group::Selector>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::group::delete::Arg {
		location: args.location.get_for_options(&args.group),
		tokens: args.group.options.tokens,
	};
	client.delete_group(&args.group.node, arg).await.map_err(
		|error| tg::error!(!error, group = %args.group.node, "failed to delete the group"),
	)?;
	Ok(())
}
