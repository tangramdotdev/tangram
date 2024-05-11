use crate::Cli;
use tangram_client as tg;
use url::Url;

/// Download a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub checksum: Option<tg::Checksum>,
	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let checksum = args.checksum.unwrap_or(tg::Checksum::Unsafe);
		let arg = tg::blob::download::Arg {
			url: args.url,
			checksum,
		};
		let blob = tg::Blob::download(&self.handle, arg).await?;
		let id = blob.id(&self.handle, None).await?;
		println!("{id}");
		Ok(())
	}
}
