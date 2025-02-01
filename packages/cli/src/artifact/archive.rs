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
	pub inner: crate::process::build::InnerArgs,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let command = artifact.archive_command(format);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.command_process_build_inner(args.inner, reference)
			.await?;
		Ok(())
	}
}
