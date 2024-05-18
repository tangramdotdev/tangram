use crate::Cli;
use crossterm::tty::IsTty;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::AsyncWriteExt as _;

/// Get a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_get(&self, args: Args) -> tg::Result<()> {
		let output = self.handle.get_build(&args.build).await?;
		let mut stdout = tokio::io::stdout();
		if stdout.is_tty() {
			let output = serde_json::to_string_pretty(&output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			stdout
				.write_all(output.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
			stdout
				.write_all(b"\n")
				.await
				.map_err(|source| tg::error!(!source, "failed to write"))?;
		} else {
			let output = serde_json::to_string(&output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			stdout
				.write_all(output.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		};
		Ok(())
	}
}
