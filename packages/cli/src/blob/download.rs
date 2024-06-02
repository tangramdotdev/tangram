use crate::Cli;
use tangram_client as tg;
use url::Url;

/// Download a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let url = args.url;
		let checksum = args.checksum.unwrap_or(tg::Checksum::Unsafe);
		let target = tg::Blob::download_target(&url, &checksum);
		let target = target.id(&self.handle).await?;
		let args = crate::target::build::Args {
			inner: crate::target::build::InnerArgs {
				target: Some(target),
				..Default::default()
			},
			detach: false,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
