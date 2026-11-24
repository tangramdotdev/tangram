use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List processes.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Fetch all pages.
	#[usage(long)]
	pub all: bool,

	/// List processes with any status.
	#[usage(long, conflicts("--status"))]
	pub any_status: bool,

	/// List the processes created by this principal. This defaults to the current principal when no owner is given.
	#[usage(conflicts("--owner", "--user", "--group", "--organization"), long)]
	pub creator: Option<tg::principal::Selector>,

	/// Continue from this cursor.
	#[usage(long)]
	pub cursor: Option<String>,

	/// Return each of the processes with its data instead of a reference.
	#[usage(long)]
	pub expand: bool,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[usage(long)]
	pub limit: Option<u64>,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[usage(flatten)]
	pub owner: tangram_cli_sandbox::Owner,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// List processes with this status.
	#[usage(default = "started", long)]
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
