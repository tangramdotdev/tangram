use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*, tangram_uri::Uri};

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

	#[arg(index = 1)]
	pub url: Uri,
}

impl Cli {
	pub async fn command_download(&mut self, mut args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
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
		let options = tg::DownloadOptions {
			checksum,
			mode: args.mode,
		};
		let command = tg::builtin::download_command(&args.url, Some(options));
		let command = command
			.store_with_handle(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let mut options = args.build;
		options.spawn.sandbox.arg.network = crate::sandbox::Network::new(true);
		let args = crate::process::build::Args {
			options,
			reference: Some(reference),
			trailing: Vec::new(),
		};
		self.command_build(args).boxed().await?;
		Ok(())
	}
}
