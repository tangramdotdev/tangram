use {crate::Cli, tangram_client::prelude::*};

/// List watches.
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
	pub output: crate::print::OutputOptions,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_watch_list(&mut self, args: Args) -> tg::Result<()> {
		// List the watches.
		let client = self.client().await?;
		let arg = tg::watch::list::Arg {
			cursor: args.cursor,
			limit: args.limit,
		};
		let output = if args.all {
			client.list_all_watches(arg).await
		} else {
			client.list_watches(arg).await
		}
		.map_err(|error| tg::error!(!error, "failed to list the watches"))?;

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
