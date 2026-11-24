use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Match specifiers.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Fetch all pages.
	#[usage(long)]
	pub all: bool,

	/// Only use cached remote results. Do not fetch from remotes.
	#[usage(long)]
	pub cached: bool,

	/// Continue from this cursor.
	#[usage(long)]
	pub cursor: Option<String>,

	#[usage(flatten)]
	pub entries: tangram_cli_list::Entries,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[usage(long)]
	pub limit: Option<u64>,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	pub pattern: tg::specifier::Pattern,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(long)]
	pub reverse: bool,

	#[usage(flatten)]
	pub time_to_live: tangram_cli_list::TimeToLive,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::match_::Arg {
		cached: args.cached,
		cursor: args.cursor,
		groups: args.entries.groups(),
		limit: args.limit,
		location: args.locations.get(),
		organizations: args.entries.organizations(),
		pattern: args.pattern.clone(),
		reverse: args.reverse,
		tags: args.entries.tags(),
		time_to_live: args.time_to_live.get(),
		tokens: tg::authorization::Tokens::default(),
		users: args.entries.users(),
	};
	let output = if args.all {
		client.match_all(arg).await
	} else {
		client.match_(arg).await
	}
	.map_err(|error| tg::error!(!error, pattern = %args.pattern, "failed to match entries"))?;
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
