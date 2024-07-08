use crate::Cli;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

/// Get a build's log.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,

	#[arg(long)]
	pub length: Option<i64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_build_log(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the log.
		let build = tg::Build::with_id(args.build);
		let arg = tg::build::log::get::Arg {
			length: args.length,
			position: args.position.map(std::io::SeekFrom::Start),
			remote: args.remote,
			size: args.size,
		};
		let mut log = build
			.log(&handle, arg)
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
