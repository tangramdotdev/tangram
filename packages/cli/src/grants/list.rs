use {crate::Cli, tangram_client::prelude::*};

/// List grants.
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
	pub location: crate::location::Args,

	#[command(flatten)]
	pub output: crate::print::OutputOptions,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// List the grants on this resource.
	#[arg(long)]
	pub resource: Option<tg::Selector<tg::Id>>,

	/// List the grants held by this subject.
	#[arg(conflicts_with = "resource", long)]
	pub subject: Option<tg::authorization::subject::Selector>,
}

impl Cli {
	pub async fn command_grants_list(&mut self, args: Args) -> tg::Result<()> {
		// List the grants.
		let client = self.client().await?;
		let subject = args.subject.is_some();
		let arg = tg::grant::list::Arg {
			cursor: args.cursor,
			limit: args.limit,
			location: args.location.get(),
			resource: args.resource,
			subject: args.subject,
		};
		let output = if args.all {
			client.list_all_grants(arg).await
		} else {
			client.list_grants(arg).await
		}
		.map_err(|error| tg::error!(!error, "failed to list the grants"))?;
		let output = output.ok_or_else(|| {
			if subject {
				tg::error!("failed to find the subject")
			} else {
				tg::error!("failed to find the resource")
			}
		})?;

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
