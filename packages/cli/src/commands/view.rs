use crate::{tui::Tui, Cli};
use tangram_client as tg;

/// View a build.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The ID of the build to view.
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let build = tg::Build::with_id(args.id);
		let tui = Tui::start(&self.handle, &build).await?;
		tui.join().await?;
		Ok(())
	}
}
