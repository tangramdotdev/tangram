use crate::Cli;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

/// Format the files in a package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_fmt(&self, mut args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.wrap_err("Failed to canonicalize the path.")?
				.try_into()?;
		}

		// Format the package.
		client.format_package(&args.package).await?;

		Ok(())
	}
}
