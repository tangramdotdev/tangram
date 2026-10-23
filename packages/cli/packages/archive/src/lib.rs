use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[arg(long)]
	pub compression: Option<tg::CompressionFormat>,

	#[arg(long)]
	pub format: tg::ArchiveFormat,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_archive(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = self.get_artifact(&args.reference).await?;
		let artifact = tg::Artifact::with_referent(artifact);
		tg::builtin::validate_archive_artifact_with_instance(&artifact, &client).await?;
		let format = args.format;
		let compression = args.compression;
		let command = tg::builtin::archive_command(&artifact, format, compression);
		let command = command
			.store_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let options = args.build;
		let transform = !options.detach && options.checkout.is_none();
		let args = tangram_cli_process::build::Args {
			options: options.clone(),
			reference: Some(reference),
			trailing: Vec::new(),
		};
		let output = tangram_cli_process::Component::new(self)
			.build(args)
			.await?;
		let output = if transform && !output.is_null() {
			let file: tg::File = output.try_into()?;
			file.contents_with_instance(&client).await?.into()
		} else {
			output
		};
		tangram_cli_process::Component::new(self)
			.print_build_output(&options, output)
			.await
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
