use crate::Cli;
use either::Either;
use tangram_client::{self as tg, Handle as _};

/// Check a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".?kind=package")]
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
		let item = self.get_reference(&args.reference).await?;

		// Get the module.
		let Either::Right(tg::Object::Directory(package)) = item else {
			return Err(tg::error!("expected a package"));
		};

		// Check the package.
		let package = package.id(&handle).await?;
		let arg = tg::package::check::Arg {
			package: package.into(),
			remote,
		};
		let output = handle.check_package(arg).await?;

		// Print the diagnostics.
		for diagnostic in &output.diagnostics {
			self.print_diagnostic(&handle, diagnostic).await;
		}

		if !output.diagnostics.is_empty() {
			return Err(tg::error!("type checking failed"));
		}

		Ok(())
	}
}
