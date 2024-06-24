use crate::Cli;
use tangram_client as tg;

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[arg(long)]
	pub format: tg::artifact::archive::Format,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let target = artifact.archive_target(format);
		let target = target.id(&client).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(target.into())),
			..Default::default()
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
