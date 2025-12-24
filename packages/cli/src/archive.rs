use {crate::Cli, tangram_client::prelude::*};

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
		let command = command
			.store(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let args = crate::build::Args {
			options: args.build,
			reference,
			trailing: Vec::new(),
		};
		self.command_build(args).await?;
		Ok(())
	}
}
