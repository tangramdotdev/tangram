use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(long, default_value_t = tg::checksum::Algorithm::Sha256)]
	pub algorithm: tg::checksum::Algorithm,

	#[command(flatten)]
	pub build: crate::build::Options,

	/// The artifact, blob, or URL to checksum.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checksum(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let referent = self.get_reference(&args.reference).await?;
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
		if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(&Either::Left(blob), algorithm);
			let command = command.id(&handle).await?;
			let reference = tg::Reference::with_object(&command.into());
			self.build(args.build, reference, vec![]).await?;
		} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(&Either::Right(artifact), algorithm);
			let command = command.id(&handle).await?;
			let reference = tg::Reference::with_object(&command.into());
			self.build(args.build, reference, vec![]).await?;
		} else {
			return Err(tg::error!("expected an artifact or a blob"));
		}
		Ok(())
	}
}
