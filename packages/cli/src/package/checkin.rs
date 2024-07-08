use crate::Cli;
use tangram_client::{self as tg, Handle};

/// Check in a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(index = 1, default_value = ".")]
	pub path: tg::Path,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_checkin(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Canonicalize the path.
		let path = tokio::fs::canonicalize(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
			.try_into()?;

		// Check in the package.
		let arg = tg::package::checkin::Arg {
			path,
			locked: args.locked,
			remote,
		};
		let tg::package::checkin::Output { package } = handle.check_in_package(arg).await?;

		// Print the package.
		println!("{package}");

		Ok(())
	}
}
