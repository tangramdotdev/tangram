use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::AsyncWriteExt as _;

/// Get a build.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::build::Id,
}

impl Cli {
	pub async fn command_build_get(&self, args: Args) -> tg::Result<()> {
		let arg = tg::build::GetArg::default();
		let output = self.handle.get_build(&args.id, arg).await?;
		let json = serde_json::to_string(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		tokio::io::stdout()
			.write_all(json.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		Ok(())
	}
}
