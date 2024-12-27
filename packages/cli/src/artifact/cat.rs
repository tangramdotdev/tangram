use crate::Cli;
use std::pin::pin;
use tangram_client::{self as tg};
use tokio::io::AsyncWriteExt as _;

/// Cat artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifacts: Vec<tg::artifact::Id>,
}

impl Cli {
	pub async fn command_artifact_cat(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let mut stdout = tokio::io::stdout();

		for artifact in args.artifacts {
			// Get the blob.
			let blob = match artifact {
				tg::artifact::Id::Directory(_) => {
					return Err(tg::error!("cannot cat a directory"));
				},
				tg::artifact::Id::File(file) => {
					let file = tg::File::with_id(file);
					file.contents(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
						.clone()
				},
				tg::artifact::Id::Symlink(symlink) => {
					let symlink = tg::Symlink::with_id(symlink);
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
							.map_err(|source| tg::error!(!source, "failed to get file contents"))?
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
		}

		// Flush.
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
