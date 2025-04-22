use crate::Cli;
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncReadExt as _;

// Put a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub bytes: Option<String>,

	#[arg(index = 1)]
	pub id: tg::process::Id,
}

impl Cli {
	pub async fn command_process_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let bytes = if let Some(bytes) = args.bytes {
			bytes
		} else {
			let mut bytes = String::new();
			tokio::io::stdin()
				.read_to_string(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};
		let data = serde_json::from_str(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deseralize the data"))?;
		let arg = tg::process::put::Arg { data };
		handle.put_process(&args.id, arg).await?;
		Ok(())
	}
}
