use crate::Cli;
use tangram_client as tg;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Format the files in a package.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(short, long, default_value = ".")]
	pub package: tg::Dependency,

	#[clap(long)]
	pub stdio: bool,
}

impl Cli {
	pub async fn command_format(&self, mut args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		if args.stdio {
			let mut text = String::new();
			tokio::io::stdin()
				.read_to_string(&mut text)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			let text = client.format(text).await?;
			tokio::io::stdout()
				.write_all(text.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			return Ok(());
		}

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Format the package.
		client.format_package(&args.package).await?;

		Ok(())
	}
}
