use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

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
			let (referent, _) = self.get_reference(reference).await?;
			let Either::Right(object) = referent.item else {
				return Err(tg::error!("expected an object"));
			};
			let object = if let Some(subpath) = &referent.subpath {
				let directory = object
					.try_unwrap_directory()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				directory.get(&handle, subpath).await?.into()
			} else {
				object
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
