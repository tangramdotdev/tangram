use {crate::Cli, tangram_client::prelude::*};

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(default_value = "sha256", long)]
	pub algorithm: tg::checksum::Algorithm,

	#[command(flatten)]
	pub build: crate::build::Options,

	/// The artifact, blob, or URL to checksum.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checksum(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let referent = self.get_reference(&args.reference).await?;
		let tg::Either::Left(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(tg::Either::Left(blob), algorithm);
			let command = command.store(&handle).await?;
			let reference = tg::Reference::with_object(command.into());
			let args = crate::build::Args {
				options: args.build,
				reference,
				trailing: Vec::new(),
			};
			self.command_build(args).await?;
		} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(tg::Either::Right(artifact), algorithm);
			let command = command.store(&handle).await?;
			let reference = tg::Reference::with_object(command.into());
			let args = crate::build::Args {
				options: args.build,
				reference,
				trailing: Vec::new(),
			};
			self.command_build(args).await?;
		} else {
			return Err(tg::error!("expected an artifact or a blob"));
		}
		Ok(())
	}
}
