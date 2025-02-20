use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tokio::io::AsyncReadExt as _;

// Put a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub id: tg::process::Id,

	#[arg(index = 2)]
	pub data: Option<String>,
}

impl Cli {
	pub async fn command_process_put(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let data = if let Some(data) = args.data {
			data
		} else {
			let mut data = String::new();
			tokio::io::stdin()
				.read_to_string(&mut data)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			data
		};
		let data = serde_json::from_str(&data)
			.map_err(|source| tg::error!(!source, "failed to deseralize the data"))?;
		let arg = tg::process::put::Arg { data };
		handle.put_process(&args.id, arg).await?;
		Ok(())
	}
}
