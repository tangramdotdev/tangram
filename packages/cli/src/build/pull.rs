use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_build_pull(&self, args: Args) -> tg::Result<()> {
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		let arg = tg::build::pull::Arg { remote };
		self.handle.pull_build(&args.build, arg).await?;
		Ok(())
	}
}
