use crate::Cli;
use either::Either;
use tangram_client as tg;

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(long, default_value_t = tg::checksum::Algorithm::Sha256)]
	pub algorithm: tg::checksum::Algorithm,

	/// The artifact, blob, or URL to checksum.
	#[arg(index = 1, default_value = ".?kind=package")]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let item = self.get_reference(&args.reference).await?;
		let Either::Right(object) = item else {
			return Err(tg::error!("expected an object"));
		};
		if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			let artifact = artifact.id(&handle).await?;
			let args = crate::artifact::checksum::Args {
				algorithm: args.algorithm,
				artifact,
			};
			self.command_artifact_checksum(args).await?;
		} else if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			let blob = blob.id(&handle).await?;
			let args = crate::blob::checksum::Args {
				algorithm: args.algorithm,
				blob,
			};
			self.command_blob_checksum(args).await?;
		} else {
			return Err(tg::error!("expected an artifact or a blob"));
		}
		Ok(())
	}
}
