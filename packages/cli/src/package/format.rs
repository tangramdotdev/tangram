use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The package to format.
	pub package: Option<tg::Dependency>,

	/// Format
	#[arg(long)]
	pub stdio: bool,
}

impl Cli {
	pub async fn command_package_format(&self, args: Args) -> tg::Result<()> {
		// Format via stdio if requested.
		if args.stdio {
			let mut text = String::new();
			tokio::io::stdin()
				.read_to_string(&mut text)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			let text = self
				.handle
				.format(text)
				.await
				.map_err(|source| tg::error!(!source, "failed to format"))?;
			tokio::io::stdout()
				.write_all(text.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			return Ok(());
		}

		// Canonicalize the package path.
		let mut package = args
			.package
			.ok_or_else(|| tg::error!("The package to format must be provided."))?;
		if let Some(path) = package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Format the package.
		self.handle.format_package(&package).await?;

		Ok(())
	}
}
