use crate::Cli;
use tangram_client as tg;

/// Checkum a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The algorithm to use.
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,

	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[command(flatten)]
	pub inner: crate::process::build::InnerArgs,
}

impl Cli {
	pub async fn command_blob_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let algorithm = args.algorithm;
		let command = blob.checksum_command(algorithm);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.command_process_build_inner(args.inner, reference)
			.await?;
		Ok(())
	}
}
