use {crate::Cli, tangram_client::prelude::*};

/// Compress a blob or a file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[command(flatten)]
	pub build: crate::build::Options,

	#[arg(long, short)]
	pub format: tg::CompressionFormat,
}

impl Cli {
	pub async fn command_compress(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let command = tg::builtin::compress_command(&blob, format);
		let command = command.store(&handle).await?;
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
