use crate::Cli;
use std::pin::pin;
use tangram_client::{self as tg};

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
			let reader = tg::Blob::with_id(blob)
				.read(&handle, tg::blob::read::Arg::default())
				.await?;

			// Copy from the reader to stdout.
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut pin!(reader), &mut writer)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the blob to stdout"))?;
		}

		Ok(())
	}
}
