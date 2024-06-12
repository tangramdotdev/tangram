use crate::Cli;
use tangram_client as tg;

/// Checkum a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub blob: tg::blob::Id,

	/// The algorithm to use.
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,
}

impl Cli {
	pub async fn command_blob_checksum(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let algorithm = args.algorithm;
		let target = blob.checksum_target(algorithm);
		let target = target.id(&self.handle).await?;
		let args = crate::target::build::Args {
			inner: crate::target::build::InnerArgs {
				target: Some(target),
				..Default::default()
			},
			detach: false,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
