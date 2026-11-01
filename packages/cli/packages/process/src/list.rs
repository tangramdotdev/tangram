use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	/// List processes with any status.
	#[arg(long, conflicts_with = "status")]
	pub any_status: bool,

	/// List the processes created by this principal. This defaults to the current principal when no owner is given.
	#[arg(long)]
	pub creator: Option<tg::principal::Selector>,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	/// Return each of the processes with its data instead of a reference.
	#[arg(long)]
	pub expand: bool,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub owner: tangram_cli_sandbox::Owner,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// List processes with this status.
	#[arg(default_value = "started", long)]
	pub status: tg::process::Status,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let creator = tangram_cli_sandbox::resolve_creator(&client, args.creator).await?;
	let owner = tangram_cli_sandbox::resolve_owner(&client, &args.owner).await?;
	let status = (!args.any_status).then_some(args.status);
	let arg = tg::process::list::Arg {
		creator,
		cursor: args.cursor,
		expand: args.expand,
		limit: args.limit,
		location: args.locations.get(),
		owner,
		status,
	};
	let output = if args.all {
		client.list_all_processes(arg).await
	} else {
		client.list_processes(arg).await
	}
	.map_err(|error| tg::error!(!error, "failed to list the processes"))?;
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
