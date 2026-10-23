use {tangram_cli_core::Cli, tangram_client::prelude::*, tangram_uri::Uri};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Download a blob or an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum_algorithm: Option<tg::checksum::Algorithm>,

	#[command(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[arg(long)]
	pub mode: Option<tg::DownloadMode>,

	#[arg(index = 1)]
	pub url: Uri,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_download(&mut self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		if args.build.spawn.checksum.is_none() {
			args.build.spawn.checksum.replace(tg::Checksum::default());
		}
		let checksum = args.checksum_algorithm.or_else(|| {
			args.build
				.spawn
				.checksum
				.as_ref()
				.map(tg::Checksum::algorithm)
		});
		let mode = args.mode.unwrap_or_default();
		let download_options = tg::DownloadOptions {
			checksum,
			mode: Some(mode),
		};
		let command = tg::builtin::download_command(&args.url, Some(download_options));
		let command = command
			.store_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let mut options = args.build;
		if options.spawn.sandbox.arg.network.get().is_none() {
			options.spawn.sandbox.arg.network =
				tangram_cli_sandbox::Network::with_network(tg::sandbox::Network::default());
		}
		let transform = !options.detach && options.checkout.is_none();
		let args = tangram_cli_process::build::Args {
			options: options.clone(),
			reference: Some(reference),
			trailing: Vec::new(),
		};
		let output = tangram_cli_process::Component::new(self)
			.build(args)
			.await?;
		let output = if transform && matches!(mode, tg::DownloadMode::Raw) && !output.is_null() {
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
