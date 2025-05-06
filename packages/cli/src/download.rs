use crate::Cli;
use tangram_client as tg;
use url::Url;

/// Download a blob or an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::build::Options,

	#[arg(long)]
	pub mode: Option<tg::DownloadMode>,

	#[arg(index = 1)]
	pub url: Url,
}

impl Cli {
	pub async fn command_download(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let options = tg::DownloadOptions { mode: args.mode };
		let command = tg::builtin::download_command(&args.url, Some(options));
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build(args.build, reference, vec![]).await?;
		Ok(())
	}
}
