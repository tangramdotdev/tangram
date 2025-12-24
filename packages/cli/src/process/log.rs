use {
	crate::Cli, futures::TryStreamExt as _, tangram_client::prelude::*,
	tokio::io::AsyncWriteExt as _,
};

/// Get a process's log.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub length: Option<i64>,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(long)]
	pub position: Option<u64>,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(long)]
	pub size: Option<u64>,

	#[arg(long)]
	pub stream: Option<tg::process::log::Stream>,
}

impl Cli {
	pub async fn command_process_log(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the log.
		let process = tg::Process::new(args.process.clone(), None, None, None, None);
		let arg = tg::process::log::get::Arg {
			length: args.length,
			local: args.local.local,
			position: args.position.map(std::io::SeekFrom::Start),
			remotes: args.remotes.remotes,
			size: args.size,
			stream: args.stream,
		};
		let mut log = process.log(&handle, arg).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to get the process log"),
		)?;

		// Print the log.
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		while let Some(chunk) = log.try_next().await? {
			stdout
				.write_all(&chunk.bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
