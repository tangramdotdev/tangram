use crate::Cli;
use futures::stream::TryStreamExt as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
use tokio_util::io::StreamReader;

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
		for artifact in args.artifacts {
			// Get the blob.
			let blob = match artifact {
				tg::artifact::Id::Directory(_) => return Err(tg::error!("cannot cat a directory")),
				tg::artifact::Id::File(file) => {
					let file = tg::File::with_id(file);
					file.contents(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
						.clone()
				},
				tg::artifact::Id::Symlink(symlink) => {
					let symlink = tg::Symlink::with_id(symlink);
					let artifact = symlink.resolve(&handle).await?;
					match artifact {
						None | Some(tg::Artifact::Symlink(_)) => {
							return Err(tg::error!("failed to resolve the symlink"))
						},
						Some(tg::Artifact::Directory(_)) => {
							return Err(tg::error!("cannot cat a directory"))
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
			let blob = blob.id(&handle).await?;
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
