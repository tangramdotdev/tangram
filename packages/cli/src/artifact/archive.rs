use crate::Cli;
use tangram_client as tg;

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<tg::artifact::archive::Format>,
	pub artifact: tg::artifact::Id,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format.unwrap_or(tg::artifact::archive::Format::Tar);
		let blob = artifact.archive(&self.handle, format).await?;
		let id = blob.id(&self.handle, None).await?;
		println!("{id}");
		Ok(())
	}
}
