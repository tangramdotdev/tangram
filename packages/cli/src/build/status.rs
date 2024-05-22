use crate::Cli;
use futures::StreamExt;
use tangram_client::{self as tg, Handle as _};

/// Get a build's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub timeout: Option<f64>,

	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_status(&self, args: Args) -> tg::Result<()> {
		// Get the status.
		let arg = tg::build::status::Arg {
			timeout: args.timeout.map(std::time::Duration::from_secs_f64),
		};
		let mut stream = self
			.handle
			.get_build_status(&args.build, arg)
			.await?
			.boxed();

		// Print the status.
		while let Some(status) = stream.next().await {
			let status = status.map_err(|source| tg::error!(!source, "expected a status"))?;
			println!("{status}");
		}

		Ok(())
	}
}
