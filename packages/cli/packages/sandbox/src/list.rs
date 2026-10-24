use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub owner: crate::Owner,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn command_sandbox_list(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let owner = crate::resolve_owner(&client, &args.owner).await?;
	let arg = tg::sandbox::list::Arg {
		cursor: args.cursor,
		limit: args.limit,
		location: args.locations.get(),
		owner,
	};
	let output = if args.all {
		client.list_all_sandboxes(arg).await
	} else {
		client.list_sandboxes(arg).await
	}
	.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?;
	if args.output.verbose {
		cli.print_serde(output, args.print).await?;
	} else {
		cli.print_serde(output.data, args.print).await?;
		if let Some(cursor) = output.cursor {
			cli.print_info_message(&format!("Next cursor: {cursor}"));
		}
	}
	Ok(())
}
