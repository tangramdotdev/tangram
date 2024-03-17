use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Check out an artifact.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The ID of the artifact to check out.
	pub id: tg::artifact::Id,

	/// The path to check out the artifact to.
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_checkout(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|error| error!(source = error, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		} else {
			path.push(args.id.to_string());
		};

		// Check out the artifact.
		let artifact = tg::Artifact::with_id(args.id.clone());
		let path = if let Some(path) = &args.path {
			Some(path.clone().try_into()?)
		} else {
			None
		};
		artifact
			.check_out(client, path.as_ref())
			.await
			.map_err(|error| error!(source = error, "failed to check out the artifact"))?;

		// Print the path.
		let path = if let Some(path) = args.path.clone() {
			path
		} else {
			client
				.path()
				.await
				.map_err(|error| error!(source = error, "failed to get the server path"))?
				.ok_or_else(|| error!("failed to get the server path"))?
				.join("artifacts")
				.join(args.id.to_string())
				.into()
		};
		println!("{}", path.display());

		Ok(())
	}
}
