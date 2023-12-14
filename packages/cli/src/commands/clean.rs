use crate::Cli;
use tangram_error::Result;

/// Remove unused objects.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	pub async fn command_clean(&self, _args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Clean.
		tg.clean().await?;

		Ok(())
	}
}
