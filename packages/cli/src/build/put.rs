use crate::Cli;
use tangram_client as tg;

use tokio::io::AsyncReadExt as _;

// Put a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub arg: Option<String>,
}

impl Cli {
	pub async fn command_build_put(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
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
		let arg: tg::build::put::Arg = serde_json::from_str(&arg)
			.map_err(|source| tg::error!(!source, "failed to deseralize"))?;
		client.put_build(&arg.id.clone(), arg.clone()).await?;
		Ok(())
	}
}
