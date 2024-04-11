use crate::Cli;
use crossterm::style::Stylize;
use tangram_client as tg;

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
	pub async fn command_check(&self, mut args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		// Canonicalize the package path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Check the package.
		let diagnostics = client.check_package(&args.package).await?;

		// Print the diagnostics.
		for diagnostic in &diagnostics {
			let diagnostic = self
				.convert_diagnostic_location(diagnostic.clone())
				.await
				.unwrap_or(diagnostic.clone());
			match diagnostic.severity {
				tg::diagnostic::Severity::Error => eprintln!("{}:", "error".red().bold()),
				tg::diagnostic::Severity::Warning => eprintln!("{}:", "warning".yellow().bold()),
				tg::diagnostic::Severity::Information => eprintln!("{}:", "info".blue().bold()),
				tg::diagnostic::Severity::Hint => eprintln!("{}:", "hint".white().bold()),
			};
			eprint!("{} {} ", "->".red(), diagnostic.message);
			if let Some(location) = &diagnostic.location {
				let (package, path) = location.module.source();
				if let Some(package) = package {
					let package = tg::Directory::with_id(package);
					let metadata = tg::package::get_metadata(client, &package).await.ok();
					let (name, version) = metadata
						.map(|metadata| (metadata.name, metadata.version))
						.unwrap_or_default();
					let name = name.as_deref().unwrap_or("<unknown>");
					let version = version.as_deref().unwrap_or("<unknown>");
					eprint!("{name}@{version}: {path}:");
				} else {
					eprint!("{path}:");
				};
				eprint!(
					"{}:{}",
					location.range.start.line + 1,
					location.range.start.character + 1,
				);
			}
			eprintln!();
		}

		if !diagnostics.is_empty() {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
