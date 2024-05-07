use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::AsyncWriteExt as _;

/// Get a build.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_get(&self, args: Args) -> tg::Result<()> {
		let output = self.handle.get_build(&args.build).await?;
		let json = serde_json::to_string(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		tokio::io::stdout()
			.write_all(json.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		Ok(())
	}
}
