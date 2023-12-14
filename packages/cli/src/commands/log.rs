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
	#[allow(clippy::unused_async)]
	pub async fn command_log(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();
		let build = tg::Build::with_id(args.id);
		let tui = Tui::start(tg, &build, tui::Options { exit: true }).await?;
		tui.join().await?;
		Ok(())
	}
}
