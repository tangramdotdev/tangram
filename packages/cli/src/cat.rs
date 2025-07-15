use crate::Cli;
use std::pin::pin;
use tangram_client as tg;
use tangram_either::Either;
use tokio::io::AsyncWriteExt as _;

/// Concatenate blobs and artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub references: Vec<tg::Reference>,
}

impl Cli {
	pub async fn command_cat(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let mut stdout = tokio::io::stdout();

		for reference in &args.references {
			let referent = self.get_reference(reference).await?;
			let Either::Right(object) = referent.item() else {
				return Err(tg::error!("expected an object"));
			};
			if let Ok(blob) = tg::Blob::try_from(object.clone()) {
				// Create a reader.
				let reader = blob.read(&handle, tg::blob::read::Arg::default()).await?;

				// Copy from the reader to stdout.
				let mut writer = tokio::io::stdout();
				tokio::io::copy(&mut pin!(reader), &mut writer)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the blob to stdout"))?;
			} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
				// Get the blob.
				let blob = match artifact {
					tg::Artifact::Directory(_) => {
						return Err(tg::error!("cannot cat a directory"));
					},
					tg::Artifact::File(file) => file
						.contents(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
						.clone(),
					tg::Artifact::Symlink(symlink) => {
						let artifact = symlink.try_resolve(&handle).await?;
						match artifact {
							None | Some(tg::Artifact::Symlink(_)) => {
								return Err(tg::error!("failed to resolve the symlink"));
							},
							Some(tg::Artifact::Directory(_)) => {
								return Err(tg::error!("cannot cat a directory"));
							},
							Some(tg::Artifact::File(file)) => file
								.contents(&handle)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to get the file contents")
								})?
								.clone(),
						}
					},
				};

				// Create a reader.
				let reader = blob.read(&handle, tg::blob::read::Arg::default()).await?;

				// Copy from the reader to stdout.
				tokio::io::copy(&mut pin!(reader), &mut stdout)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the blob contents to stdout")
					})?;
			} else {
				return Err(tg::error!("expected an artifact or a blob"));
			}
		}

		// Flush.
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
