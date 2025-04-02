use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_futures::stream::TryExt;

/// Remove unused processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_clean(&self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let stream = handle.clean().await?;
		std::pin::pin!(stream).try_last().await?;
		Ok(())
	}
}
