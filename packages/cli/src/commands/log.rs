use crate::{
	tui::{self, Tui},
	Cli,
};
use tangram_client as tg;
use tangram_error::Result;

/// Get the log for a build.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// The ID of the build to get the log for.
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_log(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		let build = tg::Build::with_id(args.id);
		let tui = Tui::start(client, &build, tui::Options { exit: true }).await?;
		tui.join().await?;

		Ok(())
	}
}
