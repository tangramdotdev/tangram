use crate::Cli;
use futures::StreamExt;
use tangram_client::{self as tg, Handle as _};

/// List a build's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub timeout: Option<f64>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_status(&self, args: Args) -> tg::Result<()> {
		let arg = tg::build::status::Arg {
			timeout: args.timeout.map(std::time::Duration::from_secs_f64),
		};
		let mut stream = self
			.handle
			.try_get_build_status(&args.build, arg)
			.await?
			.ok_or_else(|| tg::error!(%id = args.build, "expected the build to exist"))?
			.boxed();

		while let Some(status) = stream.next().await {
			let status = status.map_err(|source| tg::error!(!source, "expected a status"))?;
			println!("{status}");
		}

		Ok(())
	}
}
