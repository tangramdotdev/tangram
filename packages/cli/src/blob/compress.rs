use crate::Cli;
use tangram_client as tg;

/// Compress a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[arg(short, long)]
	pub format: tg::blob::compress::Format,
}

impl Cli {
	pub async fn command_blob_compress(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let target = blob.compress_target(format);
		let target = target.id(&client).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(target.into())),
			..Default::default()
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
