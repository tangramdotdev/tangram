use crate::Cli;
use tangram_client as tg;

/// Cat artifacts.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub artifacts: Vec<tg::artifact::Id>,
}

impl Cli {
	pub async fn command_artifact_cat(&self, args: Args) -> tg::Result<()> {
		for artifact in args.artifacts {
			// Get the blob.
			let blob = match artifact {
				tg::artifact::Id::Directory(_) => return Err(tg::error!("cannot cat a directory")),
				tg::artifact::Id::File(file) => {
					let file = tg::File::with_id(file);
					file.contents(&self.handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
						.clone()
				},
				tg::artifact::Id::Symlink(symlink) => {
					let symlink = tg::Symlink::with_id(symlink);
					let artifact = symlink.resolve(&self.handle).await?;
					match artifact {
						None | Some(tg::Artifact::Symlink(_)) => {
							return Err(tg::error!("failed to resolve the symlink"))
						},
						Some(tg::Artifact::Directory(_)) => {
							return Err(tg::error!("cannot cat a directory"))
						},
						Some(tg::Artifact::File(file)) => file
							.contents(&self.handle)
							.await
							.map_err(|source| tg::error!(!source, "failed to get file contents"))?
							.clone(),
					}
				},
			};

			// Create a reader.
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
