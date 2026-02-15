use {crate::Cli, tangram_client::prelude::*};

/// Get the latest tag matching a pattern.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	/// Set the cache TTL in seconds. Use 0 to bypass the cache.
	#[arg(long)]
	pub ttl: Option<u64>,
}

impl Cli {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::tag::list::Arg {
			cached: args.cached,
			length: Some(1),
			local: args.local.local,
			pattern: args.pattern.clone(),
			recursive: false,
			remotes: args.remotes.remotes,
			reverse: true,
			ttl: args.ttl,
		};
		let output = handle.list_tags(arg).await.map_err(
			|source| tg::error!(!source, pattern = %args.pattern, "failed to get the tag"),
		)?;
		let entry = output
			.data
			.into_iter()
			.next()
			.ok_or_else(|| tg::error!(pattern = %args.pattern, "no tag was found"))?;
		self.print_serde(entry, args.print).await?;
		Ok(())
	}
}
