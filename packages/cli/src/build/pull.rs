use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub remote: Option<String>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_pull(&self, args: Args) -> tg::Result<()> {
		let arg = tg::build::pull::Arg {
			remote: args.remote,
		};
		self.handle.pull_build(&args.build, arg).await?;
		Ok(())
	}
}
