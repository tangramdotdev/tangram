use crate::Cli;
use tangram_client as tg;

/// Decompress a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[arg(short, long)]
	pub format: tg::blob::compress::Format,

	#[command(flatten)]
	pub inner: crate::process::build::InnerArgs,
}

impl Cli {
	pub async fn command_blob_decompress(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let command = blob.decompress_command(format);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.command_process_build_inner(args.inner, reference)
			.await?;
		Ok(())
	}
}
