use {crate::Cli, tangram_client::prelude::*};

/// Get a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub tag: tg::Tag,

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
		let arg = tg::tag::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
			ttl: args.ttl,
		};
		let output = handle
			.get_tag(&args.tag, arg)
			.await
			.map_err(|source| tg::error!(!source, tag = %args.tag, "failed to get the tag"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
