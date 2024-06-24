use crate::Cli;
use tangram_client as tg;

/// Checkum a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	/// The algorithm to use.
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,
}

impl Cli {
	pub async fn command_blob_checksum(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = tg::Blob::with_id(args.blob);
		let algorithm = args.algorithm;
		let target = blob.checksum_target(algorithm);
		let target = target.id(&client).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(target.into())),
			..Default::default()
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
