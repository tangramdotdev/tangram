use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_format(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

		// Format.
		let arg = tg::format::Arg { path };
		handle.format(arg).await?;

		Ok(())
	}
}
