use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List runner tokens.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	/// Return each of the runner tokens with its data instead of a reference.
	#[arg(long)]
	pub expand: bool,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub runner: tg::runner::Id,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// List the runner tokens.
	let client = cli.client().await?;
	let arg = tg::runner::token::list::Arg {
		cursor: args.cursor,
		expand: args.expand,
		limit: args.limit,
		location: args.location.get(),
	};
	let output = if args.all {
		client.list_all_runner_tokens(&args.runner, arg).await
	} else {
		client.list_runner_tokens(&args.runner, arg).await
	}
	.map_err(|error| tg::error!(!error, "failed to list the runner tokens"))?;

	// Print the output.
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
