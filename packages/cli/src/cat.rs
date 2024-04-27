use crate::Cli;
use tangram_client as tg;

/// Output a file or a blob.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub ids: Vec<tg::Id>,
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> tg::Result<()> {
		for id in args.ids {
			let blob = if let Ok(id) = tg::blob::Id::try_from(id.clone()) {
				tg::Blob::with_id(id)
			} else if let Ok(id) = tg::file::Id::try_from(id.clone()) {
				let file = tg::File::with_id(id);
				file.contents(&self.handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get file contents"))?
					.clone()
			} else {
				return Err(tg::error!("expected a file or blob id"));
			};
			let mut reader = blob
				.reader(&self.handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the blob reader"))?;
			let mut writer = tokio::io::stdout();
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|error| {
					tg::error!(
						source = error,
						"failed to write the blob contents to stdout"
					)
				})?;
		}
		Ok(())
	}
}
