use {crate::Cli, tangram_client::prelude::*};

/// List runners.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	#[arg(long, conflicts_with = "owner")]
	pub all_owners: bool,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub output: crate::print::OutputOptions,

	#[arg(long, conflicts_with = "all_owners")]
	pub owner: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_runner_list(&mut self, args: Args) -> tg::Result<()> {
		// List the runners.
		let client = self.client().await?;
		let arg = tg::runner::list::Arg {
			all_owners: args.all_owners,
			cursor: args.cursor,
			limit: args.limit,
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
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.data, args.print).await?;
			if let Some(cursor) = output.cursor {
				self.print_info_message(&format!("Next cursor: {cursor}"));
			}
		}

		Ok(())
	}
}
