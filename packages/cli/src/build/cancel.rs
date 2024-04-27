use crate::Cli;
use tangram_client as tg;
use tg::Handle;

/// Cancel a build.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_build_cancel(&self, args: Args) -> tg::Result<()> {
		let arg = tg::build::finish::Arg {
			outcome: tg::build::outcome::Data::Canceled,
		};
		self.handle.finish_build(&args.id, arg).await?;
		Ok(())
	}
}
