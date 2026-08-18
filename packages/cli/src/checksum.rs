use {crate::Cli, tangram_client::prelude::*};

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(default_value = "sha256", long)]
	pub algorithm: tg::checksum::Algorithm,

	#[command(flatten)]
	pub build: crate::process::build::Options,

	/// The blob or file to checksum.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checksum(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let referent = self.get_with_follow(&args.reference).await?;
		let object = referent
			.into_graph_edge()?
			.node
			.try_unwrap_object()
			.map_err(|_| tg::error!("expected a blob or file"))?;
		let input = if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			tg::Either::Left(blob)
		} else if let Ok(file) = tg::File::try_from(object) {
			tg::Either::Right(file)
		} else {
			return Err(tg::error!("expected a blob or file"));
		};
		let command = tg::builtin::checksum_command(input, args.algorithm);
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
		let output = if transform && !output.is_null() {
			let file: tg::File = output.try_into()?;
			file.text_with_handle(&client).await?.into()
		} else {
			output
		};
		self.print_build_output(&options, output).await
	}
}
