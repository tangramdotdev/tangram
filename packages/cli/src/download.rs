use {crate::Cli, tangram_client::prelude::*, tangram_uri::Uri};

/// Download a blob or an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum_algorithm: Option<tg::checksum::Algorithm>,

	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(long)]
	pub mode: Option<tg::DownloadMode>,

	/// When extracting, allow an entry to replace one that was already extracted, as tar does.
	#[arg(long)]
	pub overwrite: bool,

	#[arg(index = 1)]
	pub url: Uri,
}

impl Cli {
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
			overwrite: args.overwrite.then_some(true),
		};
		let command = tg::builtin::download_command(&args.url, Some(download_options));
		let command = command
			.store_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let mut options = args.build;
		if options.spawn.sandbox.arg.network.get().is_none() {
			options.spawn.sandbox.arg.network =
				crate::sandbox::Network::with_network(tg::sandbox::Network::default());
		}
		let transform = !options.detach && options.checkout.is_none();
		let args = crate::process::build::Args {
			options: options.clone(),
			reference: Some(reference),
			trailing: Vec::new(),
		};
		let output = self.build(args).await?;
		let output = if transform && matches!(mode, tg::DownloadMode::Raw) && !output.is_null() {
			let file: tg::File = output.try_into()?;
			file.contents_with_handle(&client).await?.into()
		} else {
			output
		};
		self.print_build_output(&options, output).await
	}
}
