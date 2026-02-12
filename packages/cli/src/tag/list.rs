use {crate::Cli, tangram_client::prelude::*};

/// List tags.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1, default_value = "*")]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(long)]
	pub reverse: bool,

	/// Set the cache TTL in seconds. Use 0 to bypass the cache.
	#[arg(long)]
	pub ttl: Option<u64>,
}

impl Cli {
	pub async fn command_tag_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::tag::list::Arg {
			length: None,
			local: args.local.local,
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			remotes: args.remotes.remotes,
			reverse: args.reverse,
			ttl: args.ttl,
		};
		let output = handle.list_tags(arg).await.map_err(
			|source| tg::error!(!source, pattern = %args.pattern, "failed to list the tags"),
		)?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
