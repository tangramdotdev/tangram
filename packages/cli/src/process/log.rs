use crate::Cli;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

/// Get a process's log.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub length: Option<i64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_process_log(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the log.
		let process = tg::Process::new(args.process, None, None, None, None);
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::process::log::get::Arg {
			length: args.length,
			position: args.position.map(std::io::SeekFrom::Start),
			remote,
			size: args.size,
		};
		let mut log = process
			.log(&handle, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process log"))?;

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
