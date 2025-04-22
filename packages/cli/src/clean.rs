use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Remove unused processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_clean(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let stream = handle.clean().await?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
