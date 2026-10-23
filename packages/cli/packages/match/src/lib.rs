use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Match specifiers.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Fetch all pages.
	#[arg(long)]
	pub all: bool,

	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	/// Continue from this cursor.
	#[arg(long)]
	pub cursor: Option<String>,

	#[command(flatten)]
	pub entries: tangram_cli_list::Entries,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[arg(long)]
	pub limit: Option<u64>,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[arg(index = 1)]
	pub pattern: tg::specifier::Pattern,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(long)]
	pub reverse: bool,

	#[command(flatten)]
	pub ttl: tangram_cli_list::Ttl,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_match(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
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
			tokens: tg::authorization::Tokens::default(),
			ttl: args.ttl.get(),
			users: args.entries.users(),
		};
		let output = if args.all {
			client.match_all(arg).await
		} else {
			client.match_(arg).await
		}
		.map_err(|error| tg::error!(!error, pattern = %args.pattern, "failed to match entries"))?;
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

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
