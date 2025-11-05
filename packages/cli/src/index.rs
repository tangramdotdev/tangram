use {crate::Cli, tangram_client::prelude::*};

/// Index processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_index(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let stream = handle.index().await?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
