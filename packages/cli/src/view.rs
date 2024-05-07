use crate::{tui::Tui, Cli};
use tangram_client as tg;

/// View a build.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The build to view.
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let build = tg::Build::with_id(args.build);
		let tui = Tui::start(&self.handle, &build).await?;
		tui.wait().await?;
		Ok(())
	}
}
