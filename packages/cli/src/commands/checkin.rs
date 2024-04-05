use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;

/// Check in an artifact.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The path to check in.
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_checkin(&self, args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Perform the checkin.
		let artifact = tg::Artifact::check_in(client, &path.try_into()?).await?;

		// Print the ID.
		let id = artifact.id(client).await?;
		println!("{id}");

		Ok(())
	}
}
