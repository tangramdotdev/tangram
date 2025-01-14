use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tokio::io::AsyncReadExt as _;

// Put a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub arg: Option<String>,
}

impl Cli {
	pub async fn command_process_put(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = if let Some(arg) = args.arg {
			arg
		} else {
			let mut arg = String::new();
			tokio::io::stdin()
				.read_to_string(&mut arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			arg
		};
		let arg: tg::process::put::Arg = serde_json::from_str(&arg)
			.map_err(|source| tg::error!(!source, "failed to deseralize"))?;
		handle.put_process(&arg.id.clone(), arg.clone()).await?;
		Ok(())
	}
}
