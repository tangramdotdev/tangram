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
	pub inner: crate::command::build::InnerArgs,
}

impl Cli {
	pub async fn command_blob_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let algorithm = args.algorithm;
		let command = blob.checksum_command(algorithm);
		let command = command.id(&handle).await?;
		let args = crate::command::build::Args {
			reference: Some(tg::Reference::with_object(&command.into())),
			inner: args.inner,
		};
		self.command_command_build(args).await?;
		Ok(())
	}
}
