use crate::Cli;
use tangram_client as tg;

/// Cancel a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_build_cancel(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::build::finish::Arg {
			outcome: tg::build::outcome::Data::Canceled,
			remote,
		};
		client.finish_build(&args.build, arg).await?;
		Ok(())
	}
}
