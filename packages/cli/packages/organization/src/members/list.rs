use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List organization members.
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
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub organization: tg::Referent<tg::organization::Selector>,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// List the organization members.
	let client = cli.client().await?;
	let arg = tg::organization::members::list::Arg {
		cursor: args.cursor,
		limit: args.limit,
		location: args.location.get_for_options(&args.organization),
		tokens: args.organization.options.tokens,
	};
	let output = if args.all {
		client
			.list_all_organization_members(&args.organization.node, arg)
			.await
	} else {
		client
			.list_organization_members(&args.organization.node, arg)
			.await
	}
	.map_err(|error| tg::error!(!error, "failed to list the organization members"))?;

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
