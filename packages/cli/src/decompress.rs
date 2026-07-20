use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Decompress a blob or a file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub input: tg::Reference,

	#[command(flatten)]
	pub build: crate::process::build::Options,
}

impl Cli {
	pub async fn command_decompress(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let input = self.get_resolved_object(&args.input).await?;
		let input = match tg::Object::with_referent(input) {
			tg::Object::Blob(blob) => tg::Either::Left(blob),
			tg::Object::File(file) => tg::Either::Right(file),
			_ => return Err(tg::error!("expected a blob or file")),
		};
		let command = tg::builtin::decompress_command(input);
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
