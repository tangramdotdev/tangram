use crate::Cli;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

/// Get a build's log.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<i64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[arg(long)]
	pub size: Option<u64>,

	#[arg(long)]
	pub timeout: Option<f64>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_log(&self, args: Args) -> tg::Result<()> {
		// Get the log.
		let build = tg::Build::with_id(args.build);
		let arg = tg::build::log::Arg {
			length: args.length,
			position: args.position.map(std::io::SeekFrom::Start),
			size: args.size,
			timeout: args.timeout.map(std::time::Duration::from_secs_f64),
		};
		let mut log = build
			.log(&self.handle, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the build log"))?;

		// Print the log.
		let mut stdout = tokio::io::stdout();
		while let Some(chunk) = log.try_next().await? {
			stdout
				.write_all(&chunk.bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
		}

		Ok(())
	}
}
