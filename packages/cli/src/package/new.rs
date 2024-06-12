use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;

/// Create a new package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The name of the package. Defaults to the directory name.
	#[arg(long)]
	pub name: Option<String>,

	/// The directory to initialize the package in.
	pub path: Option<PathBuf>,

	/// The version of the package. Defaults to "0.0.0".
	#[arg(long, default_value = "0.0.0")]
	pub version: String,
}

impl Cli {
	pub async fn command_package_new(&self, args: Args) -> tg::Result<()> {
		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Create a directory at the path.
		tokio::fs::create_dir_all(&path).await.map_err(|source| {
			let path = path.display();
			tg::error!(!source, %path, "failed to create the directory")
		})?;

		// Init.
		self.command_package_init(super::init::Args {
			path: args.path,
			name: args.name,
			version: args.version,
		})
		.await?;

		Ok(())
	}
}
