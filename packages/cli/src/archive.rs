use {crate::Cli, tangram_client::prelude::*};

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(long)]
	pub compression: Option<tg::CompressionFormat>,

	#[arg(long)]
	pub format: tg::ArchiveFormat,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_archive(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = self.get_artifact(&args.reference).await?;
		let artifact = tg::Artifact::with_referent(artifact);
		tg::builtin::validate_archive_artifact_with_handle(&artifact, &client).await?;
		let format = args.format;
		let compression = args.compression;
		let command = tg::builtin::archive_command(&artifact, format, compression);
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
			file.contents_with_handle(&client).await?.into()
		} else {
			output
		};
		self.print_build_output(&options, output).await
	}
}
