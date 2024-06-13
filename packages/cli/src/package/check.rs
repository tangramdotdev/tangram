use crate::Cli;
use tangram_client as tg;

/// Check a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_check(&self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Check the package.
		let arg = tg::package::check::Arg {
			locked: args.locked,
		};
		let output = client.check_package(&args.package, arg).await?;

		// Print the diagnostics.
		for diagnostic in &output.diagnostics {
			self.print_diagnostic(diagnostic).await;
		}

		if !output.diagnostics.is_empty() {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
