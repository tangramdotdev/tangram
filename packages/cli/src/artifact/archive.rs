use crate::Cli;
use tangram_client as tg;

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(long)]
	pub format: tg::artifact::archive::Format,

	#[arg(long)]
	pub compression_format: Option<tg::blob::compress::Format>,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let compression_format = args.compression_format;
		let command = artifact.archive_command(format, compression_format);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build_process(args.build, reference, vec![]).await?;
		Ok(())
	}
}
