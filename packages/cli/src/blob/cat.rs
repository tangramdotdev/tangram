use crate::Cli;
use futures::TryStreamExt as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
use tokio_util::io::StreamReader;

/// Cat blobs.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blobs: Vec<tg::blob::Id>,
}

impl Cli {
	pub async fn command_blob_cat(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		for blob in args.blobs {
			// Create a reader.
			let stream = handle
				.try_read_blob(&blob, tg::blob::read::Arg::default())
				.await?
				.ok_or_else(|| tg::error!("expected a blob"))?;
			let mut reader = StreamReader::new(
				stream
					.map_ok(|chunk| chunk.bytes)
					.map_err(std::io::Error::other),
			);

			// Copy from the reader to stdout.
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut pin!(reader), &mut writer)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to write the blob contents to stdout")
				})?;
		}

		Ok(())
	}
}
