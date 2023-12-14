use crate::Cli;
use tangram_client as tg;
use tangram_error::Result;

/// Pull an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub id: tg::object::Id,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_pull(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Pull the object.
		tg.pull_object(&args.id).await?;

		Ok(())
	}
}
