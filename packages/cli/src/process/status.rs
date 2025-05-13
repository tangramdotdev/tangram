use crate::Cli;
use futures::StreamExt as _;
use std::pin::pin;
use tangram_client::{self as tg, prelude::*};

/// Get a process's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_status(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the stream.
		let stream = handle.get_process_status(&args.process).await?;

		// Print the status.
		let mut stream = pin!(stream);
		while let Some(status) = stream.next().await {
			let status = status.map_err(|source| tg::error!(!source, "expected a status"))?;
			println!("{status}");
		}

		Ok(())
	}
}
