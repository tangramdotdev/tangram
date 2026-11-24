use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// List runners.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Fetch all pages.
	#[usage(long)]
	pub all: bool,

	#[usage(long, conflicts("--owner"))]
	pub all_owners: bool,

	/// Continue from this cursor.
	#[usage(long)]
	pub cursor: Option<String>,

	/// Return each of the runners with its data instead of a reference.
	#[usage(long)]
	pub expand: bool,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[usage(long)]
	pub limit: Option<u64>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[usage(long, conflicts("--all-owners"))]
	pub owner: Option<tg::principal::Selector>,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// List the runners.
	let client = cli.client().await?;
	let arg = tg::runner::list::Arg {
		all_owners: args.all_owners,
		cursor: args.cursor,
		expand: args.expand,
		limit: args.limit,
		location: args.location.get(),
		owner: args.owner,
	};
	let output = if args.all {
		client.list_all_runners(arg).await
	} else {
		client.list_runners(arg).await
	}
	.map_err(|error| tg::error!(!error, "failed to list the runners"))?;

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
