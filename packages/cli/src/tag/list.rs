use {crate::Cli, tangram_client::prelude::*};

/// List tags.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1, default_value = "*")]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,

	#[arg(long)]
	pub reverse: bool,

	/// Set the cache TTL in seconds. Use 0 to bypass the cache.
	#[arg(long)]
	pub ttl: Option<u64>,
}

impl Cli {
	pub async fn command_tag_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::list::Arg {
			cached: args.cached,
			length: None,
			location: args.locations.get(),
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			reverse: args.reverse,
			ttl: args.ttl,
		};
		let output = client.list_tags(arg).await.map_err(
			|source| tg::error!(!source, pattern = %args.pattern, "failed to list the tags"),
		)?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
