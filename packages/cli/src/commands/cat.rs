use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Output a file or a blob.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub ids: Vec<tg::Id>,
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;
		for id in args.ids {
			let blob = if let Ok(id) = tg::blob::Id::try_from(id.clone()) {
				tg::Blob::with_id(id)
			} else if let Ok(id) = tg::file::Id::try_from(id.clone()) {
				let file = tg::File::with_id(id);
				file.contents(client)
					.await
					.map_err(|source| error!(!source, "failed to get file contents"))?
					.clone()
			} else {
				return Err(error!("expected a file or blob id"));
			};
			let mut reader = blob
				.reader(client)
				.await
				.map_err(|source| error!(!source, "failed to create the blob reader"))?;
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|error| {
					error!(
						source = error,
						"failed to write the blob contents to stdout"
					)
				})?;
		}
		Ok(())
	}
}
