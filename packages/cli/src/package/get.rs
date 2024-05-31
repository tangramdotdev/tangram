use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client::{self as tg, Handle as _};

/// Get a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,
}

impl Cli {
	pub async fn command_package_get(&self, args: Args) -> tg::Result<()> {
		let arg = tg::package::get::Arg {
			metadata: true,
			yanked: true,
			path: true,
			..Default::default()
		};
		let output = self.handle.get_package(&args.package, arg).await.map_err(
			|error| tg::error!(source = error, %dependency = args.package, "failed to get the package"),
		)?;

		if matches!(output.yanked, Some(true)) {
			eprintln!("{}", "YANKED".red().bold());
		}

		if let Some(metadata) = output.metadata {
			let name = metadata.name.as_deref().unwrap_or("<unknown>");
			let version = metadata.version.as_deref().unwrap_or("<unknown>");
			eprintln!("{} {name}@{version}", "info".blue().bold());
			if let Some(description) = &metadata.description {
				eprintln!("{} {}", "info".blue().bold(), description);
			}
		}

		if let Some(path) = output.path {
			eprintln!("{} at {}", "info".blue().bold(), path);
		}

		Ok(())
	}
}
