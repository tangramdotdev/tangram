use crate::Cli;
use tangram_client as tg;

/// Checkum a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The algorithm to use.
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,

	pub blob: tg::blob::Id,
}

impl Cli {
	pub async fn command_blob_checksum(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let algorithm = args.algorithm;
		let target = blob.checksum_target(algorithm);
		let target = target.id(&self.handle, None).await?;
		let args = crate::target::build::InnerArgs {
			target: Some(target),
			..Default::default()
		};
		let output = self.command_target_build_inner(args, false).await?;
		let output = output.unwrap().unwrap_value();
		println!("{output}");
		Ok(())
	}
}
