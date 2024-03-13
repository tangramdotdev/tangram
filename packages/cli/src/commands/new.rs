use crate::Cli;
use std::path::PathBuf;
use tangram_error::{error, Result};

/// Create a new package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// The directory to initialize the package in.
	pub path: Option<PathBuf>,

	/// The name of the package. Defaults to the directory name.
	#[arg(long)]
	pub name: Option<String>,

	/// The version of the package. Defaults to "0.0.0".
	#[arg(long, default_value = "0.0.0")]
	pub version: String,
}

impl Cli {
	pub async fn command_new(&self, args: Args) -> Result<()> {
		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|error| error!(source = error, "Failed to get the working directory."))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Create a directory at the path.
		tokio::fs::create_dir_all(&path).await.map_err(|error| {
			let path = path.display();
			error!(source = error, %path, "Failed to create the directory.")
		})?;

		// Init.
		self.command_init(super::init::Args {
			path: args.path,
			name: args.name,
			version: args.version,
		})
		.await?;

		Ok(())
	}
}
