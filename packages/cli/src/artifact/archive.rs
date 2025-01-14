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

	#[command(flatten)]
	pub inner: crate::command::build::InnerArgs,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let command = artifact.archive_command(format);
		let target = command.id(&handle).await?;
		let args = crate::command::build::Args {
			reference: Some(tg::Reference::with_object(&target.into())),
			inner: args.inner,
		};
		self.command_command_build(args).await?;
		Ok(())
	}
}
