use crate::Cli;
use tangram_client::{self as tg, Handle};

/// Create a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_blob_create(&self, _args: Args) -> tg::Result<()> {
		let reader = tokio::io::stdin();
		let tg::blob::create::Output { blob } = self.handle.create_blob(reader).await?;
		println!("{blob}");
		Ok(())
	}
}
