use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Compress a blob or a file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub input: tg::Either<tg::blob::Id, tg::file::Id>,

	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(long, short)]
	pub format: tg::CompressionFormat,
}

impl Cli {
	pub async fn command_compress(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let input = match args.input {
			tg::Either::Left(id) => tg::Either::Left(tg::Blob::with_id(id)),
			tg::Either::Right(id) => tg::Either::Right(tg::File::with_id(id)),
		};
		let format = args.format;
		let command = tg::builtin::compress_command(input, format);
		let command = command
			.store_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let args = crate::process::build::Args {
			options: args.build,
			reference: Some(reference),
			trailing: Vec::new(),
		};
		self.command_build(args).boxed().await?;
		Ok(())
	}
}
