use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Check a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_check(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let object = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory.get(&handle, subpath).await?.into()
		} else {
			object
		};
		let Ok(package) = tg::Directory::try_from(object) else {
			return Err(tg::error!("expected a directory"));
		};
		let package = package.id(&handle).await?;

		// Check the module.
		let arg = tg::package::check::Arg { package, remote };
		let output = handle.check_package(arg).await?;

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
