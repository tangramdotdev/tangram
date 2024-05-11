use crate::Cli;
use tangram_client as tg;

/// Cat blobs.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub blobs: Vec<tg::blob::Id>,
}

impl Cli {
	pub async fn command_blob_cat(&self, args: Args) -> tg::Result<()> {
		for blob in args.blobs {
			// Create a reader.
			let blob = tg::Blob::with_id(blob);
			let mut reader = blob
				.reader(&self.handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the blob reader"))?;

			// Copy from the reader to stdout.
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to write the blob contents to stdout")
				})?;
		}

		Ok(())
	}
}
