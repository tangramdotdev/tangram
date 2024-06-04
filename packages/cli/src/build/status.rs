use crate::Cli;
use futures::StreamExt;
use tangram_client::{self as tg, Handle as _};

/// Get a build's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_status(&self, args: Args) -> tg::Result<()> {
		// Get the status.
		let mut stream = self.handle.get_build_status(&args.build).await?.boxed();

		// Print the status.
		while let Some(status) = stream.next().await {
			let status = status.map_err(|source| tg::error!(!source, "expected a status"))?;
			println!("{status}");
		}

		Ok(())
	}
}
