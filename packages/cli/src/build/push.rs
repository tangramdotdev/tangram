use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Push a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_build_push(&self, args: Args) -> tg::Result<()> {
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::build::push::Arg { remote };
		self.handle.push_build(&args.build, arg).await?;
		Ok(())
	}
}
