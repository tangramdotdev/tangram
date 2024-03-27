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

	/// Overwrite any files or directories at the check out path if they exist.
	#[arg(short, long, requires = "path")]
	pub force: bool,
}

impl Cli {
	pub async fn command_checkout(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		} else {
			path.push(args.id.to_string());
		};

		// Check out the artifact.
		let artifact = tg::Artifact::with_id(args.id.clone());
		let options = if let Some(path) = args.path.clone() {
			let path = if path.is_absolute() {
				path.try_into()?
			} else {
				path.parent()
					.and_then(|path| (!path.as_os_str().is_empty()).then_some(path))
					.unwrap_or(".".as_ref())
					.canonicalize()
					.map_err(
						|source| error!(!source, %path = path.display(), "failed to canonicalize the path"),
					)?
					.join(
						path.file_name()
							.ok_or_else(|| error!(%path = path.display(), "invalid path"))?,
					)
					.try_into()?
			};
			let force = args.force;
			Some(tg::artifact::CheckOutOptions { path, force })
		} else {
			None
		};

		artifact
			.check_out(client, options)
			.await
			.map_err(|source| error!(!source, "failed to check out the artifact"))?;

		// Print the path.
		let path = if let Some(path) = args.path.clone() {
			path
		} else {
			client
				.path()
				.await
				.map_err(|source| error!(!source, "failed to get the server path"))?
				.ok_or_else(|| error!("failed to get the server path"))?
				.join("artifacts")
				.join(args.id.to_string())
				.into()
		};
		println!("{}", path.display());

		Ok(())
	}
}
