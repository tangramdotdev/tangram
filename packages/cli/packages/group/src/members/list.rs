use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// List group members.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Fetch all pages.
	#[usage(long)]
	pub all: bool,

	/// Continue from this cursor.
	#[usage(long)]
	pub cursor: Option<String>,

	pub group: tg::Referent<tg::group::Selector>,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[usage(long)]
	pub limit: Option<u64>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// List the group members.
	let client = cli.client().await?;
	let arg = tg::group::members::list::Arg {
		cursor: args.cursor,
		limit: args.limit,
		location: args.location.get_for_options(&args.group),
		tokens: args.group.options.tokens,
	};
	let output = if args.all {
		client.list_all_group_members(&args.group.node, arg).await
	} else {
		client.list_group_members(&args.group.node, arg).await
	}
	.map_err(|error| tg::error!(!error, "failed to list the group members"))?;

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
