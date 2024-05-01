use crate::Cli;
use futures::StreamExt;
use tangram_client as tg;
use tokio::io::AsyncWriteExt;

/// Get a build's log.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_build_log(&self, args: Args) -> tg::Result<()> {
		let build = tg::Build::with_id(args.id);
		let mut chunks = build
			.log(&self.handle, tg::build::log::GetArg::default())
			.await
			.map_err(
				|source| tg::error!(!source, %build = build.id(), "failed to get build log"),
			)?;

		let mut stdout = tokio::io::stdout();
		while let Some(chunk) = chunks.next().await {
			let chunk = chunk.map_err(|source| tg::error!(!source, "failed to get log chunk"))?;
			stdout
				.write_all(&chunk.bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdout"))?;
		}
		Ok(())
	}
}
