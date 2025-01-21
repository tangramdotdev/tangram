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
	pub inner: crate::command::run::InnerArgs,
}

impl Cli {
	pub async fn command_blob_decompress(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let command = blob.decompress_command(format);
		let command = command.id(&handle).await?;
		let args = crate::command::build::Args {
			reference: Some(tg::Reference::with_object(&command.into())),
			inner: args.inner,
		};
		self.command_command_build(args).await?;
		Ok(())
	}
}
