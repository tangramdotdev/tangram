use crate::Cli;
use either::Either;
use tangram_client as tg;

/// Concatenate blobs and artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub references: Vec<tg::Reference>,
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		for reference in &args.references {
			let item = self.get_reference(reference).await?;
			let Either::Right(object) = item else {
				return Err(tg::error!("expected an object"));
			};
			if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
				let artifact = artifact.id(&handle).await?;
				self.command_artifact_cat(crate::artifact::cat::Args {
					artifacts: vec![artifact],
				})
				.await?;
			} else if let Ok(blob) = tg::Blob::try_from(object.clone()) {
				let blob = blob.id(&handle).await?;
				self.command_blob_cat(crate::blob::cat::Args { blobs: vec![blob] })
					.await?;
			} else {
				return Err(tg::error!("expected an artifact or a blob"));
			}
		}
		Ok(())
	}
}
