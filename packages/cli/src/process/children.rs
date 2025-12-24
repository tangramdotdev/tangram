use {
	crate::Cli,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	tangram_client::prelude::*,
};

/// Get a process's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<u64>,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(long)]
	pub position: Option<u64>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_process_children(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::children::get::Arg {
			length: args.length,
			local: args.local.local,
			position: args.position.map(std::io::SeekFrom::Start),
			remotes: args.remotes.remotes,
			size: args.size,
		};
		let stream = handle
			.get_process_children(&args.process, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.process, "failed to get the process children"),
			)?
			.map_ok(|chunk| stream::iter(chunk.data.into_iter().map(Ok)))
			.try_flatten();
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
