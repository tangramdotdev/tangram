use {crate::Cli, tangram_client::prelude::*};

/// Decompress a blob or a file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_decompress(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let input = self.resolve_object(&args.reference).await?;
		let (input, blob) = match tg::Object::with_referent(input) {
			tg::Object::Blob(blob) => (tg::Either::Left(blob), true),
			tg::Object::File(file) => (tg::Either::Right(file), false),
			_ => return Err(tg::error!("expected a blob or file")),
		};
		let command = tg::builtin::decompress_command(input);
		let command = command
			.store_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let options = args.build;
		let transform = !options.detach && options.checkout.is_none();
		let args = crate::process::build::Args {
			options: options.clone(),
			reference: Some(reference),
			trailing: Vec::new(),
		};
		let output = self.build(args).await?;
		let output = if transform && blob && !output.is_null() {
			let file: tg::File = output.try_into()?;
			file.contents_with_handle(&client).await?.into()
		} else {
			output
		};
		self.print_build_output(&options, output).await
	}
}
