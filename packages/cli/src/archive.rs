use crate::Cli;
use tangram_client as tg;

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub build: crate::build::Options,

	#[arg(long)]
	pub format: tg::ArchiveFormat,

	#[arg(long)]
	pub compression: Option<tg::CompressionFormat>,
}

impl Cli {
	pub async fn command_archive(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let compression = args.compression;
		let command = tg::builtin::archive_command(&artifact, format, compression);
		let command = command.store(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build(args.build, reference, vec![], true).await?;
		Ok(())
	}
}
