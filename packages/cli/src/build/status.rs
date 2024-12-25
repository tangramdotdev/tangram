use crate::Cli;
use futures::StreamExt as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};

/// Get a build's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_status(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the status.
		let stream = handle.get_build_status(&args.build).await?;

		// Print the status.
		let mut stream = pin!(stream);
		while let Some(status) = stream.next().await {
			let status = status.map_err(|source| tg::error!(!source, "expected a status"))?;
			println!("{status}");
		}

		Ok(())
	}
}
