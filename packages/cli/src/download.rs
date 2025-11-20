use {crate::Cli, tangram_client::prelude::*, tangram_uri::Uri};

/// Download a blob or an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum_algorithm: Option<tg::checksum::Algorithm>,

	#[command(flatten)]
	pub build: crate::build::Options,

	#[arg(long)]
	pub mode: Option<tg::DownloadMode>,

	#[arg(index = 1)]
	pub url: Uri,
}

impl Cli {
	pub async fn command_download(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let checksum_algorithm = args.checksum_algorithm.or_else(|| {
			args.build
				.spawn
				.checksum
				.as_ref()
				.map(tg::Checksum::algorithm)
		});
		let options = tg::DownloadOptions {
			checksum_algorithm,
			mode: args.mode,
		};
		let command = tg::builtin::download_command(&args.url, Some(options));
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
