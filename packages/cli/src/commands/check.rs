use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Check a package for errors.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[clap(long)]
	pub locked: bool,

	#[clap(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_check(&self, mut args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Check the package.
		let diagnostics = client.check_package(&args.package).await?;

		// Print the diagnostics.
		for diagnostic in &diagnostics {
			// Get the diagnostic location and message.
			let tg::Diagnostic {
				location, message, ..
			} = diagnostic;

			// Print the location if one is available.
			if let Some(location) = location {
				println!("{location}");
			}

			// Print the diagnostic message.
			println!("{message}");

			// Print a newline.
			println!();
		}

		if !diagnostics.is_empty() {
			return Err(error!("type checking failed"));
		}

		Ok(())
	}
}
