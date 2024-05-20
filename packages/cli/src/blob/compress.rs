use crate::Cli;
use tangram_client as tg;

/// Compress a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub format: tg::blob::compress::Format,

	pub blob: tg::blob::Id,
}

impl Cli {
	pub async fn command_blob_compress(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let target = blob.compress_target(format);
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
