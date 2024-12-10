use crate::Cli;
use tangram_client::{self as tg, Handle as _};

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
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Cancel the build.
		let arg = tg::build::finish::Arg {
			outcome: tg::build::outcome::Data::Cancelation(tg::build::outcome::data::Cancelation {
				reason: Some("the build was explicitly canceled".to_owned()),
			}),
			remote,
		};
		handle.finish_build(&args.build, arg).await?;

		Ok(())
	}
}
