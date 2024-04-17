use crate::{tui::Tui, Cli};
use tangram_client as tg;

/// Get the log for a build.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The ID of the build to get the log for.
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_log(&self, args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		let build = tg::Build::with_id(args.id);
		let tui = Tui::start(client, &build).await?;
		tui.join().await?;

		Ok(())
	}
}
