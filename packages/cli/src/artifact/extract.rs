use crate::Cli;
use tangram_client as tg;

/// Extract an artifact from a blob.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<tg::artifact::archive::Format>,
	pub blob: tg::blob::Id,
}

impl Cli {
	pub async fn command_artifact_extract(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let blob = tg::Artifact::extract(&self.handle, &blob, format).await?;
		let id = blob.id(&self.handle, None).await?;
		println!("{id}");
		Ok(())
	}
}
