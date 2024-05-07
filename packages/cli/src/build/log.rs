use crate::Cli;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

/// Get a build's log.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_log(&self, args: Args) -> tg::Result<()> {
		// Get the build.
		let build = tg::Build::with_id(args.build);

		// Get the log.
		let arg = tg::build::log::Arg::default();
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
