use crate::Cli;
use tangram_client as tg;

/// Decompress a blob.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub format: tg::blob::compress::Format,

	pub blob: tg::blob::Id,
}

impl Cli {
	pub async fn command_blob_decompress(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let blob = blob.decompress(&self.handle, args.format).await?;
		let blob = blob.id(&self.handle, None).await?;
		println!("{blob}");
		Ok(())
	}
}
