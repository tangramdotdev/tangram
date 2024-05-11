use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path to check in.
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_artifact_checkin(&self, args: Args) -> tg::Result<()> {
		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Perform the checkin.
		let path = path.try_into()?;
		let artifact = tg::Artifact::check_in(&self.handle, path).await?;

		// Print the ID.
		let id = artifact.id(&self.handle, None).await?;
		println!("{id}");

		Ok(())
	}
}
