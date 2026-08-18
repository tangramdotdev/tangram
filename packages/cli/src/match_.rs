use {crate::Cli, tangram_client::prelude::*};

/// Match specifiers.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub entries: crate::list::Entries,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub pattern: tg::specifier::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub reverse: bool,

	#[command(flatten)]
	pub ttl: crate::list::Ttl,
}

impl Cli {
	pub async fn command_match(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::match_::Arg {
			cached: args.cached,
			groups: args.entries.groups(),
			length: None,
			location: args.locations.get(),
			organizations: args.entries.organizations(),
			pattern: args.pattern.clone(),
			reverse: args.reverse,
			tags: args.entries.tags(),
			tokens: tg::authorization::Tokens::default(),
			ttl: args.ttl.get(),
			users: args.entries.users(),
		};
		let output = client.match_(arg).await.map_err(
			|error| tg::error!(!error, pattern = %args.pattern, "failed to match entries"),
		)?;
		self.print_serde(output.data, args.print).await?;

		Ok(())
	}
}
