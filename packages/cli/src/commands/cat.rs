use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

/// Write the contents of a list of objects to stdout.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub ids: Vec<tg::Id>,
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> Result<()> {
		let tg = self.client().await?;
		let tg = &tg;
		let Args { ids } = args;

		// Get the blobs.
		for id in ids {
			let blob = 'a: {
				if let Ok(id) = tg::blob::Id::try_from(id.clone()) {
					break 'a tg::Blob::with_id(id);
				}

				if let Ok(id) = tg::file::Id::try_from(id.clone()) {
					let file = tg::File::with_id(id);
					let blob = file
						.contents(tg)
						.await
						.wrap_err("Failed to get file contents.")?
						.clone();
					break 'a blob;
				}

				return Err(error!("Expected a file or blob id."));
			};

			let mut reader = blob
				.reader(tg)
				.await
				.wrap_err("Failed to create the blob reader.")?;
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.wrap_err("Failed to write the blob contents to stdout.")?;
		}

		Ok(())
	}
}
