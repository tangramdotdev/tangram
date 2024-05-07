use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Format a package.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The package to format.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// Format
	#[arg(long)]
	pub stdio: bool,
}

impl Cli {
	pub async fn command_package_format(&self, mut args: Args) -> tg::Result<()> {
		// Format via stdio if requested.
		if args.stdio {
			let mut text = String::new();
			tokio::io::stdin()
				.read_to_string(&mut text)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			let text = self.handle.format(text).await?;
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
		self.handle.format_package(&args.package).await?;

		Ok(())
	}
}
